package fileindexer

import (
	"encoding/json"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"os"
	"path/filepath"
	"time"
	"log"
)

type Indexer struct {
	baseDir string
	db      *leveldb.DB
	err     error
	dbMeta  *DbMeta
	readingSequence int64
	writingSequence int64
}

func (v *Indexer) OpenOrCreate() {
	v.db, v.err = leveldb.OpenFile(v.baseDir+"fileIndexerDb", nil)
	if v.err != nil {
		return
	}
	v.dbMeta = v.getDbMeta()

	if v.dbMeta == nil {
		v.dbMeta = &DbMeta{v.baseDir, 0}
	}
	v.readingSequence = v.dbMeta.Sequence
	v.writingSequence = v.readingSequence + 1
}

func (v *Indexer) Close() {
	if v.db != nil {
		v.db.Close()
		v.db = nil
	}
}

func (v *Indexer) GetDbMeta() *DbMeta {
	return v.dbMeta
}

func (v *Indexer) GetFileMeta(path string, isDir bool) *FileMeta {
	data, err := v.db.Get([]byte(keyForPath(path, isDir)), nil)
	if err != nil {
		if err != leveldb.ErrNotFound {
			log.Fatal("Unexpected error:", err)
		}
		return nil
	}
	var meta FileMeta
	err = json.Unmarshal(data, &meta)
	if err != nil {
		log.Fatal("Unexpected error:", err)
	}
	return &meta
}

func (v *Indexer) getDbMeta() *DbMeta {
	data, err := v.db.Get([]byte("."), nil)
	if err != nil {
		if err != leveldb.ErrNotFound {
			log.Fatal("Unexpected error:", err)
		}
		return nil
	}
	var meta DbMeta
	err = json.Unmarshal(data, &meta)
	if err != nil {
		log.Fatal("Unexpected error:", err)
	}
	return &meta
}

func (v *Indexer) Update() error {
	// Updating dirs and files
	fmt.Println("updating:" + v.baseDir)
	fileInfo, err := os.Lstat(v.baseDir)
	if err != nil {
		log.Fatal("Lstat failed on " + v.baseDir)
	}
	v.updateDir(v.baseDir, fileInfo)
	fmt.Println("updated")

	// Commiting new sequence
	v.dbMeta.Sequence = v.writingSequence
	v.readingSequence = v.writingSequence
	v.writingSequence ++
	v.putKeyValue(".", v.dbMeta)

	// Removing obsoleted dir/file.
	removedFileCount := 0
	removedDirCount := 0
	removedItems := make([]string, 100)
	v.Iter(func(path string, meta *FileMeta) {
		if meta.Sequence != v.dbMeta.Sequence {
			removedItems = append(removedItems, path)
			if meta.IsDir {
				removedDirCount ++
			} else {
				removedFileCount ++
			}
		}
	})
	fmt.Println("removed files:", removedFileCount)
	fmt.Println("removed dirs:", removedDirCount)
	for _, path := range removedItems {
		v.db.Delete([]byte(path), nil)
	}
	return nil
}

type IterFunc func(path string, meta *FileMeta)

func (v *Indexer) Iter(iterFunc IterFunc) {
	iter := v.db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		var meta FileMeta
		err := json.Unmarshal(iter.Value(), &meta)
		if err != nil {
			iterFunc(key, nil)
		} else {
			iterFunc(key, &meta)
		}
	}
	iter.Release()
}

func (v *Indexer) updateDir(dir string, info os.FileInfo) (totalFileCount, totalFileSize int64) {
	totalFileCount = 0
	totalFileSize = 0
	relativePath := dir[len(v.baseDir):]
	if relativePath == "/fileIndexerDb" {
		return
	}
	if filepath.Base(relativePath) == "@eaDir" {
		return
	}
	fmt.Println("updating:" + relativePath)
	dirInfo := &DirInfo{time.Now(), time.Time{}, 0, 0}
	meta := FileMeta{
		info.Size(),
		info.IsDir(),
		"",
		info.ModTime(),
		v.writingSequence,
		dirInfo}

	infos, err := readDir(dir)
	if err != nil {
		log.Fatal("readDir failed on " + dir)
	}

	for _, info := range infos {
		if info.IsDir() {
			count, size := v.updateDir(filepath.Join(dir, info.Name()), info)
			totalFileCount += count
			totalFileSize += size
		} else {
			totalFileCount += 1
			totalFileSize += info.Size()
			v.updateFile(filepath.Join(dir, info.Name()), info)
		}
	}
	dirInfo.TotalFileCount = totalFileCount
	dirInfo.TotalFileSize = totalFileSize
	v.putKeyValue(keyForPath(relativePath,true), meta)
	return
}

func (v *Indexer) updateFile(file string, info os.FileInfo) {
	relativePath := file[len(v.baseDir):]
	meta := FileMeta {
		info.Size(),
		false,
		"",
		info.ModTime(),
		v.writingSequence,
		nil}
	v.putKeyValue(keyForPath(relativePath, false), meta)
}

func (v *Indexer) putKeyValue(key string, value interface{}) error {
	json, err := json.Marshal(value)
	if err != nil {
		return err
	}
	v.db.Put([]byte(key), json, nil)
	return nil
}

func NewIndexer(baseDir string) *Indexer {
	indexer := Indexer{baseDir, nil, nil, nil, 0, 0}
	indexer.OpenOrCreate()
	return &indexer
}

func keyForPath(path string, isDir bool) string {
	prefix := "f"
	if isDir {
		prefix = "d"
	}
	return prefix + path
}

func readDir(dirname string) ([]os.FileInfo, error) {
	f, err := os.Open(dirname)
	if err != nil {
		return nil, err
	}
	fileInfo, err := f.Readdir(0)
	f.Close()
	if err != nil {
		return nil, err
	}
	return fileInfo, nil
}
