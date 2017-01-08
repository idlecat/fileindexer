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
	RemovedDirCount  int32
	RemovedFileCount int32
	RemovedFileSize  int64
}

func (v *RepositoryInfo) Add(other *RepositoryInfo) {
	if other == nil {
		return
	}
	v.FileCount += other.FileCount
	v.FileSize += other.FileSize
	v.DirCount += other.DirCount
	v.ChangedFileCount += other.ChangedFileCount
	v.ChangedFileSize += other.ChangedFileSize
	v.RemovedDirCount += other.RemovedDirCount
	v.RemovedFileCount += other.RemovedFileCount
	v.RemovedFileSize += other.RemovedFileSize
}

const (
	PREFIX_FILE = 'f'
	PREFIX_DIR  = 'd'
	PREFIX_HASH = 'h'
	KEY_DB_META = "."
)

func (v *Indexer) OpenOrCreate(indexDir string) {
	v.db, v.err = leveldb.OpenFile(indexDir, nil)
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

func (v *Indexer) GetFileMeta(relativePath string) *protos.FileMeta {
	var meta protos.FileMeta
	if v.getProto(keyForPath(relativePath, false), &meta) {
		return &meta
	} else {
		return nil
	}
}

func (v *Indexer) GetDirMeta(relativePath string) *protos.FileMeta {
	var meta protos.FileMeta
	if v.getProto(keyForPath(relativePath, true), &meta) {
		return &meta
	} else {
		return nil
	}
}

func (v *Indexer) getDbMeta() *protos.DbMeta {
	var meta protos.DbMeta
	if v.getProto(KEY_DB_META, &meta) {
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
	info := v.updateDir(v.baseDir, fileInfo)

	// Commiting new sequence
	v.dbMeta.Sequence = v.writingSequence
	v.readingSequence = v.writingSequence
	v.writingSequence++
	v.putKeyValue(KEY_DB_META, v.dbMeta)

	// Removing obsoleted dir/file from index.
	var removedFileCount int32 = 0
	var removedFileSize int64 = 0
	var removedDirCount int32 = 0
	removedItems := make([]*protos.FileMeta, 0, 100)
	v.Iter(func(path string, meta *protos.FileMeta) {
		if meta.Sequence != v.dbMeta.Sequence {
			meta.RelativePath = path
			removedItems = append(removedItems, meta)
			if meta.IsDir {
				removedDirCount++
			} else {
				removedFileCount++
				removedFileSize += meta.Size
			}
		}
	})
	info.RemovedFileCount = removedFileCount
	info.RemovedFileSize = removedFileSize
	info.RemovedDirCount = removedDirCount
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
		if key[0] != PREFIX_DIR && key[0] != PREFIX_FILE {
			continue
		}
		var meta protos.FileMeta
		proto.Unmarshal(iter.Value(), &meta)
		iterFunc(key[1:], &meta)
	}
	iter.Release()
}

func (v *Indexer) updateDir(dir string, info os.FileInfo) *RepositoryInfo {
	fmt.Println("updating dir:" + dir)
	if v.shouldSkipPath(dir) {
		return nil
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

	rInfo := RepositoryInfo{}
	for _, info := range infos {
		if info.IsDir() {
			rInfo.Add(v.updateDir(filepath.Join(dir, info.Name()), info))
			rInfo.DirCount += 1
		} else {
			rInfo.Add(v.updateFile(filepath.Join(dir, info.Name()), info))
		}
	}
	dirInfo.TotalFileCount = rInfo.FileCount
	dirInfo.TotalFileSize = rInfo.FileSize
	dirInfo.UpdateTimeEnd = int32(time.Now().Unix())
	v.putFileOrDirMeta(dir, true, &meta)
	return &rInfo
}

func (v *Indexer) updateFile(file string, info os.FileInfo) *RepositoryInfo {
	rInfo := RepositoryInfo{
		FileCount: 1,
		FileSize:  info.Size(),
	}
	relativePath := v.getRelativePath(file)
	meta := v.GetFileMeta(relativePath)
	md5sum := ""
	var err error
	if meta == nil || meta.Size != info.Size() || meta.ModTime != int32(info.ModTime().Unix()) {
		// calculates hash for new/changed file.
		md5sum, err = hashFile(file)
		if err != nil {
			log.Print(err)
			return nil
		}
	} else {
		md5sum = meta.Md5Sum
	}

	newMeta := protos.FileMeta{
		Size:     info.Size(),
		IsDir:    false,
		Md5Sum:   md5sum,
		ModTime:  int32(info.ModTime().Unix()),
		Sequence: v.writingSequence,
	}
	v.putFileOrDirMeta(file, false, &newMeta)
	if meta == nil || meta.Md5Sum != md5sum {
		// need to update hash entry.
		if meta != nil {
			v.removeHash(meta.Md5Sum, relativePath)
		}
		v.addHash(md5sum, relativePath)
		rInfo.ChangedFileCount = 1
		rInfo.ChangedFileSize = info.Size()
	}
	return &rInfo
}

func (v *Indexer) removeItem(meta *protos.FileMeta) {
	key := keyForPath(meta.RelativePath, meta.IsDir)
	v.db.Delete([]byte(key), nil)
	if !meta.IsDir {
		v.removeHash(meta.Md5Sum, meta.RelativePath)
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

func (v *Indexer) putFileOrDirMeta(path string, isDir bool, meta proto.Message) {
	relativePath := v.getRelativePath(path)
	v.putKeyValue(keyForPath(relativePath, isDir), meta)
}

func (v *Indexer) getRelativePath(path string) string {
	relativePath := path[len(v.baseDir):]
	// strip left-most '/' from relative path. relativePath for root directory is
	// ''.
	if len(relativePath) > 0 && relativePath[0] == '/' {
		relativePath = relativePath[1:]
	}
	return relativePath
}

func keyForHash(hash string) string {
	return string(PREFIX_HASH) + hash
}

func (v *Indexer) addHash(md5sum string, relativePath string) {
	var paths protos.FilePaths
	key := keyForHash(md5sum)
	if v.getProto(key, &paths) {
		for _, path := range paths.Paths {
			if path == relativePath {
				return
			}
		}
	} else {
		paths = protos.FilePaths{}
	}
	paths.Paths = append(paths.Paths, relativePath)
	v.putKeyValue(key, &paths)
}

func (v *Indexer) removeHash(md5sum string, relativePath string) {
	var paths protos.FilePaths
	key := keyForHash(md5sum)
	if !v.getProto(key, &paths) {
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
		v.db.Delete([]byte(key), nil)
	} else {
		s[len(s)-1], s[index] = s[index], s[len(s)-1]
		paths.Paths = s[:len(s)-1]
		v.putKeyValue(key, &paths)
	}
}

func (v *Indexer) GetFilesByHash(hash string) []string {
	var paths protos.FilePaths
	key := keyForHash(hash)
	if v.getProto(key, &paths) {
		return paths.Paths
	} else {
		return nil
	}
}

func hashFile(filePath string) (string, error) {
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

func NewIndexer(baseDir string, indexDir string) *Indexer {
	if indexDir == "" {
		indexDir = path.Join(baseDir, "fileIndexerDb")
	}
	indexer := Indexer{baseDir, nil, nil, nil, 0, 0}
	indexer.OpenOrCreate(indexDir)
	return &indexer
}

func keyForPath(path string, isDir bool) string {
	prefix := PREFIX_FILE
	if isDir {
		prefix = PREFIX_DIR
	}
	return string(prefix) + path
}
