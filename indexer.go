package fileindexer

import (
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"path/filepath"
)

type Indexer struct {
	baseDir string
	db      *leveldb.DB
	err     error
}

func (v *Indexer) Create() {
	v.db, v.err = leveldb.OpenFile(v.baseDir+"fileIndexerDb", nil)
}

func (v *Indexer) Close() {
	if v.db != nil {
		v.db.Close()
		v.db = nil
	}
}

func (v *Indexer) Update() bool {
	fmt.Println("baseDir:" + v.baseDir)
	filepath.Walk(v.baseDir, v.visit)
	return true
}

func (v *Indexer) GetFileMeta(path string) (*FileMeta, error) {
	data, err := v.db.Get([]byte(path), nil)
	if err != nil {
		return nil, err
	}
	var meta FileMeta
	return &meta, json.Unmarshal(data, &meta)
}

func (v *Indexer) visit(path string, f os.FileInfo, err error) error {
	fmt.Printf("Visit: %s\n", path)
	meta := FileMeta{
		path,
		f.Size(),
		f.IsDir(),
		"",
		f.ModTime()}
	json, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	v.db.Put([]byte(path), json, nil)
	return nil
}

func NewIndexer(baseDir string) *Indexer {
	indexer := Indexer{baseDir, nil, nil}
	indexer.Create()
	return &indexer
}
