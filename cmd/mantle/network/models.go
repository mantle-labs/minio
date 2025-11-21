package network

import "io"

type PutFileResp struct {
	Id string
	//other fields are available.
}

type NamedReader struct {
	R    *io.Reader
	Name string
}
