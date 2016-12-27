package fileindexer

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/idlecat/fileindexer/protos"
	"github.com/syndtr/goleveldb/leveldb"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"path/filepath"
	"time"
)

type Indexer struct {
	baseDir         string
	db              *leveldb.DB
	err             error
	dbMeta          *protos.DbMeta
	readingSequence int32
	writingSequence int32
}

type RepositoryInfo struct {
	FileCount        int32
	FileSize         int64
	DirCount         int32
	ChangedFileCount int32
	ChangedFileSize  int64
}

const (
	PREFIX_FILE = 'f'
	PREFIX_DIR  = 'd'
	PREFIX_HASH = 'h'
)

func (v *Indexer) OpenOrCreate() {
	v.db, v.err = leveldb.OpenFile(path.Join(v.baseDir, "fileIndexerDb"), nil)
	if v.err != nil {
		return
	}
	v.dbMeta = v.getDbMeta()

	if v.dbMeta == nil {
		v.dbMeta = &protos.DbMeta{
			BaseDir:  v.baseDir,
			Sequence: 0,
		}
	}
	v.readingSequence = v.dbMeta.Sequence
	v.writingSequence = v.readingSequence + 1
	log.Printf("Open indexer db with sequence %d", v.readingSequence)
}

// Quickly scan the directory to get file numbers and total size.
func (v *Indexer) QuickScan(info *RepositoryInfo) {
	fileInfo, err := os.Lstat(v.baseDir)
	if err != nil {
		log.Fatal("Lstat failed on " + v.baseDir)
	}
	v.quickScanInternal(v.baseDir, fileInfo, info)
}

func (v *Indexer) shouldSkipPath(path string) bool {
	relativePath := path[len(v.baseDir):]
	if relativePath == "/fileIndexerDb" {
		return true
	}
	if filepath.Base(relativePath) == "@eaDir" {
		return true
	}
	return false
}

func (v *Indexer) quickScanInternal(dir string, info os.FileInfo, rInfo *RepositoryInfo) {
	if v.shouldSkipPath(dir) {
		return
	}

	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal("readDir failed on " + dir)
	}

	for _, info := range infos {
		if info.IsDir() {
			v.quickScanInternal(filepath.Join(dir, info.Name()), info, rInfo)
			rInfo.DirCount += 1
		} else {
			rInfo.FileCount += 1
			rInfo.FileSize += info.Size()
		}
	}
}

func (v *Indexer) Close() {
	if v.db != nil {
		v.db.Close()
		v.db = nil
	}
}

func (v *Indexer) GetDbMeta() *protos.DbMeta {
	return v.dbMeta
}

func (v *Indexer) GetError() error {
	return v.err
}

func (v *Indexer) GetFileMeta(relativePath string, isDir bool) *protos.FileMeta {
	var meta protos.FileMeta
	if v.getProto(keyForPath(relativePath, isDir), &meta) {
		return &meta
	} else {
		return nil
	}
}

func (v *Indexer) getDbMeta() *protos.DbMeta {
	var meta protos.DbMeta
	if v.getProto(".", &meta) {
		return &meta
	} else {
		return nil
	}
}

func (v *Indexer) getProto(key string, msg proto.Message) bool {
	data, err := v.db.Get([]byte(key), nil)
	if err != nil {
		if err != leveldb.ErrNotFound {
			log.Fatal("Unexpected error:", err)
		}
		return false
	}
	err = proto.Unmarshal(data, msg)
	if err != nil {
		log.Fatal("Unmarshal failed")
	}
	return true
}

func (v *Indexer) Update() error {
	// Updating dirs and files
	fileInfo, err := os.Lstat(v.baseDir)
	if err != nil {
		log.Fatal("Lstat failed on " + v.baseDir)
	}
	v.updateDir(v.baseDir, fileInfo)

	// Commiting new sequence
	v.dbMeta.Sequence = v.writingSequence
	v.readingSequence = v.writingSequence
	v.writingSequence++
	v.putKeyValue(".", v.dbMeta)

	// Removing obsoleted dir/file from index.
	removedFileCount := 0
	removedDirCount := 0
	removedItems := make([]*protos.FileMeta, 100)
	v.Iter(func(path string, meta *protos.FileMeta) {
		if meta.Sequence != v.dbMeta.Sequence {
			meta.RelativePath = path
			removedItems = append(removedItems, meta)
			if meta.IsDir {
				removedDirCount++
			} else {
				removedFileCount++
			}
		}
	})
	fmt.Println("removed files:", removedFileCount)
	fmt.Println("removed dirs:", removedDirCount)
	for _, meta := range removedItems {
		v.removeItem(meta)
	}
	return nil
}

type IterFunc func(path string, meta *protos.FileMeta)

func (v *Indexer) Iter(iterFunc IterFunc) {
	iter := v.db.NewIterator(nil, nil)
	for iter.Next() {
		key := string(iter.Key())
		if key[0] == PREFIX_HASH {
			continue
		}
		var meta protos.FileMeta
		proto.Unmarshal(iter.Value(), &meta)
		iterFunc(key[1:], &meta)
	}
	iter.Release()
}

func (v *Indexer) updateDir(dir string, info os.FileInfo) (totalFileCount, totalFileSize int64) {
	fmt.Println("updating dir:" + dir)
	totalFileCount = 0
	totalFileSize = 0
	if v.shouldSkipPath(dir) {
		return
	}
	dirInfo := &protos.DirInfo{
		UpdateTimeStart: int32(time.Now().Unix()),
	}
	meta := protos.FileMeta{
		Size:     info.Size(),
		IsDir:    true,
		ModTime:  int32(info.ModTime().Unix()),
		Sequence: v.writingSequence,
		DirInfo:  dirInfo}

	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal("readDir failed on " + dir)
	}

	for _, info := range infos {
		log.Print(info.Name())
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
	relativePath := dir[len(v.baseDir):]
	v.putKeyValue(keyForPath(relativePath, true), &meta)
	return
}

func (v *Indexer) updateFile(file string, info os.FileInfo) {
	md5sum, err := md5Sum(file)
	if err != nil {
		log.Print(err)
		return
	}
	relativePath := file[len(v.baseDir):]
	meta := protos.FileMeta{
		Size:     info.Size(),
		IsDir:    false,
		Md5Sum:   md5sum,
		ModTime:  int32(info.ModTime().Unix()),
		Sequence: v.writingSequence,
	}
	v.putKeyValue(keyForPath(relativePath, false), &meta)
	v.addMd5Sum(md5sum, relativePath)
}

func (v *Indexer) removeItem(meta *protos.FileMeta) {
	key := keyForPath(meta.RelativePath, meta.IsDir)
	v.db.Delete([]byte(key), nil)
	if !meta.IsDir {
		v.removeMd5Sum(meta.Md5Sum, meta.RelativePath)
	}
}

func (v *Indexer) putKeyValue(key string, value proto.Message) {
	json, err := proto.Marshal(value)
	if err != nil {
		log.Fatal("Failed to marshal value for key:" + key)
	}
	err = v.db.Put([]byte(key), json, nil)
	if err != nil {
		log.Fatal("Writing db failed at key:" + key)
	}
}

func (v *Indexer) addMd5Sum(md5sum string, relativePath string) {
	var paths protos.FilePaths
	if v.getProto(md5sum, &paths) {
		for _, path := range paths.Paths {
			if path == relativePath {
				return
			}
		}
	} else {
		paths = protos.FilePaths{}
	}
	paths.Paths = append(paths.Paths, relativePath)
	v.putKeyValue(string(PREFIX_HASH)+md5sum, &paths)
}

func (v *Indexer) removeMd5Sum(md5sum string, relativePath string) {
	var paths protos.FilePaths
	if !v.getProto(md5sum, &paths) {
		log.Printf("md5sum not found for %s", relativePath)
		return
	}
	index := -1
	for i, path := range paths.Paths {
		if path == relativePath {
			index = i
			break
		}
	}
	if index == -1 {
		log.Printf("md5sum not found for %s", relativePath)
		return
	}
	s := paths.Paths
	if len(s) == 1 {
		v.db.Delete([]byte(md5sum), nil)
	} else {
		s[len(s)-1], s[index] = s[index], s[len(s)-1]
		paths.Paths = s[:len(s)-1]
		v.putKeyValue(md5sum, &paths)
	}
}

func md5Sum(filePath string) (string, error) {
	var returnMD5String string
	file, err := os.Open(filePath)
	if err != nil {
		return returnMD5String, err
	}
	defer file.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return returnMD5String, err
	}
	hashInBytes := hash.Sum(nil)[:16]
	returnMD5String = hex.EncodeToString(hashInBytes)
	return returnMD5String, nil
}

func NewIndexer(baseDir string) *Indexer {
	indexer := Indexer{baseDir, nil, nil, nil, 0, 0}
	indexer.OpenOrCreate()
	return &indexer
}

func keyForPath(path string, isDir bool) string {
	prefix := PREFIX_FILE
	if isDir {
		prefix = PREFIX_DIR
	}
	return string(prefix) + path
}
