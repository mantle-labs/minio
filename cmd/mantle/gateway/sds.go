package gateway

import (
	"encoding/json"
	"fmt"
	"github.com/minio/minio/cmd/mantle/network"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

func Put(f *os.File, fn string) (string, error) {
	client := &http.Client{}
	val := map[string]io.Reader{
		"file":        f,
		"DisplayName": strings.NewReader(fn),
	}

	putResp, err := network.UploadFormData(client, urlJoin("files"), val, setMantleHeaders())
	if err != nil {
		//TODO:handle
		return "", nil
	}

	return putResp.Id, nil
}

func Get(r io.Reader) (bb []byte, err error) {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return
	}

	client := &http.Client{}
	resp, err := network.Get(client, urlJoin("files", string(b)), setMantleHeaders())
	if err != nil {
		return nil, err
	}

	bb, err = ioutil.ReadAll(resp.Body)
	if err != nil {
		return
	}

	return
}

type sdsFileInfo struct {
	//More fields are available.
	Size int64  `json:"size"`
	Id   string `json:"id"`
}

func GetFileSize(id string) (s int64, err error) {
	client := &http.Client{}
	resp, err := network.Get(client, urlJoin("files/info", id), setMantleHeaders())
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

	if fi.Size > 0 {
		return fi.Size, nil
	}

	fmt.Println("cannot find file in mantle sds")
	return 0, nil
}
