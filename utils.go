package fileindexer

import (
	"crypto/md5"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
)

const (
	NORMAL             = 0
	STOP_SCAN_THIS_DIR = 1
)

type DirScanFunc func(path string, info os.FileInfo) int

func ScanDir(dir string, callback DirScanFunc) {
	fileInfo, err := os.Lstat(dir)
	if err != nil {
		log.Fatal("Lstat failed on " + dir)
	}
	scanDirInternal(dir, fileInfo, callback)
}

func scanDirInternal(dir string, info os.FileInfo, callback DirScanFunc) {
	ret := callback(dir, info)
	if ret == STOP_SCAN_THIS_DIR {
		return
	}
	infos, err := ioutil.ReadDir(dir)
	if err != nil {
		log.Fatal("readDir failed on " + dir)
	}

	for _, info := range infos {
		path := filepath.Join(dir, info.Name())
		if info.IsDir() {
			scanDirInternal(path, info, callback)
		} else {
			callback(path, info)
		}
	}
}

func HashFile(filePath string) (string, error) {
	var ret string
	file, err := os.Open(filePath)
	if err != nil {
		return ret, err
	}
	defer file.Close()
	hash := md5.New()
	if _, err := io.Copy(hash, file); err != nil {
		return ret, err
	}
	hashInBytes := hash.Sum(nil)[:16]
	ret = hex.EncodeToString(hashInBytes)
	return ret, nil
}
