package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/minio/minio/cmd/mantle/network"
)

type sdsFile struct {
	ID           string `json:"id"`
	Pointer      string `json:"pointer"`
	FileName     string `json:"fileName"`
	CreationDate string `json:"creationDate"`
}

type sdsFileInfo struct {
	//More fields are available.
	Size            int64  `json:"size"`
	UnencryptedSize int64  `json:"unencryptedSize"`
	Id              string `json:"id"`
}

func Shard(ctx context.Context, p string, configId string, object string, fsOpenFile func(ctx context.Context, readPath string, offset int64) (io.ReadCloser, int64, error)) error {
	r, _, err := fsOpenFile(ctx, p, 0)
	if err != nil {
		return err
	}

	id, err := Put(r, object, configId)
	if err != nil {
		return err
	}
	r.Close()

	err = os.Remove(p)
	if err != nil {
		return err
	}

	f, err := os.Create(p)
	defer f.Close()
	if err != nil {
		return err
	}

	n, err := f.Write([]byte(id))
	if err != nil || n != len(id) {
		return err
	}

	return nil
}

func Put(f io.Reader, fn string, configId string) (string, error) {
	client := &http.Client{}
	nr := network.NamedReader{&f, fn}

	putResp, err := network.UploadFormData(client, urlJoin("files"), nr, setMantleHeaders(configId))
	if err != nil {
		//TODO:handle
		fmt.Println(err.Error())
		return "", err
	}

	return putResp.Id, nil
}

func Get(r io.Reader, configId string) (readCloser io.ReadCloser, bodyLength int64, err error) {
	fileId, err := GetId(r)
	if err != nil {
		return nil, 0, err
	}

	client := &http.Client{}
	resp, err := network.Get(client, urlJoin("files", fileId), setMantleHeaders(configId))
	if err != nil {
		return nil, 0, err
	}

	return resp.Body, resp.ContentLength, nil
}

func GetFileSize(id string) (s int64, err error) {
	client := &http.Client{}
	resp, err := network.Get(client, urlJoin("files/info", id), setMantleHeaders(""))
	if err != nil {
		return 0, err
	}

	fi := sdsFileInfo{Id: id}
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	err = json.Unmarshal(b, &fi)
	if err != nil {
		return
	}

	if fi.UnencryptedSize > 0 {
		return fi.UnencryptedSize, nil
	}

	fmt.Println("cannot find file in mantle sds")
	return 0, nil
}

func GetFilesByBatch(base *url.URL, offset int, limit int) (*[]sdsFile, error) {
	client := &http.Client{}
	params := url.Values{}
	params.Set("limit", fmt.Sprintf("%d", limit))
	params.Set("offset", fmt.Sprintf("%d", offset))
	reqURL := *base
	reqURL.RawQuery = params.Encode()

	resp, err := network.Get(client, reqURL.String(), setMantleHeaders(""))
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Response HTTP status code error: %d", resp.StatusCode)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var batchFiles []sdsFile

	err = json.Unmarshal(body, &batchFiles)
	if err != nil {
		return nil, err
	}

	return &batchFiles, nil
}

func cleanUpAndLogError(tempDir string, errMsg string, err error) {
	fmt.Printf("\nFailed Recovery: %s: %v\n", errMsg, err)
	if tempDir != "" {
		fmt.Println("Deleting temporary recovery directory...")
		removeTempErr := os.RemoveAll(tempDir)
		if removeTempErr != nil {
			fmt.Printf("Error removing temporary recovery directory: %v\n", removeTempErr)
		} else {
			fmt.Println("Temporary recovery directory deleted successfully")
		}
	}
}

func Recovery(root string) {
	fmt.Println("Starting mantle recovery")
	fmt.Println("root is at : ", root)
	timestamp := time.Now().Format("20060102_150405")
	// To avoid partial recovery, we write in a temporary directory first
	tempRecoveryDir := filepath.Join(root, timestamp+"_recovery_tmp")
	finalRecoveryDir := filepath.Join(root, timestamp+"_recovery")

	err := os.MkdirAll(tempRecoveryDir, os.ModePerm)
	if err != nil {
		cleanUpAndLogError("", "Error creating temporary recovery directory", err)
		return
	}

	//Get batch of 5000 files
	offset := 0
	limit := 5000

	doneCount := 0

	base, err := url.Parse(urlJoin("files"))
	if err != nil {
		cleanUpAndLogError(tempRecoveryDir, "Error setting the url", err)
		return
	}

	for {
		batchFiles, err := GetFilesByBatch(base, offset, limit)
		if err != nil {
			cleanUpAndLogError(tempRecoveryDir, "Error getting batch", err)
			return
		}

		//When nothing is returned, the recovery is completed
		if len(*batchFiles) == 0 {
			// When its done, we rename the temporary directory to the final directory
			err = os.Rename(tempRecoveryDir, finalRecoveryDir)
			if err != nil {
				cleanUpAndLogError(tempRecoveryDir, "Error renaming recovery directory", err)
				return
			}
			fmt.Println("\nRecovery completed")
			break
		}

		for idx, file := range *batchFiles {
			fullPath := filepath.Join(tempRecoveryDir, file.FileName)
			err = os.MkdirAll(filepath.Dir(fullPath), os.ModePerm)
			if err != nil {
				cleanUpAndLogError(tempRecoveryDir, "Error creating directory", err)
				return
			}

			err = os.WriteFile(fullPath, []byte(file.ID), 0644)
			if err != nil {
				if pathErr, ok := err.(*os.PathError); ok && pathErr.Err == syscall.EISDIR {
					fmt.Printf("Skipping writing %s because destination is a directory: %v\n", fullPath, err)
					continue
				}
				cleanUpAndLogError(tempRecoveryDir, "Error writing file", err)
				return
			}

			doneCount++
			// Log every 500 files to reduce log pollution
			if doneCount%500 == 0 || idx == len(*batchFiles)-1 {
				fmt.Printf("\rProcessed %d/%d in batch, (%d done files)", idx+1, len(*batchFiles), doneCount)
			}
		}

		offset += limit
	}
}

type StorageStatus struct {
	Host   string `json:"host"`
	Status string `json:"status"`
	Region string `json:"region"`
}

func Health(configId string) ([]StorageStatus, error) {
	client := &http.Client{}
	resp, err := network.Get(client, urlJoin("health"), setMantleHeaders(configId))
	if err != nil {
		return []StorageStatus{}, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		fmt.Println("Error reading response body:", err)
		return []StorageStatus{}, err
	}

	healthCheck := []StorageStatus{}
	err = json.Unmarshal(body, &healthCheck)
	if err != nil {
		fmt.Println("Error parsing JSON:", err)
		return []StorageStatus{}, err
	}

	return healthCheck, nil
}
