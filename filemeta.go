package fileindexer

import (
	"time"
)

type FileMeta struct {
	Name    string
	Size    int64
	IsDir   bool
	Md5Sum  string
	ModTime time.Time
}
