package fileindexer

import (
	"time"
)

type FileMeta struct {
	Size    int64
	IsDir   bool
	Md5Sum  string
	ModTime time.Time
	Sequence int64
	DirInfo *DirInfo
}

type DirInfo struct {
	UpdateTimeStart time.Time
	UpdateTimeEnd time.Time
	TotalFileSize int64
	TotalFileCount int64
}

type DbMeta struct {
	BaseDir string
	Sequence int64
}