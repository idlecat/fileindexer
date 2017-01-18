package fileindexer

import (
	"bufio"
	"crypto/md5"
	"encoding/hex"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
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

func RemoveFileSafely(relativePath string, origDir string, destDir string) {
	// Move origDir/path_to_file/file to destDir/path_to_file/file
	pathToFile := filepath.Dir(relativePath)
	err := os.MkdirAll(filepath.Join(destDir, pathToFile), os.ModeDir|0700)
	if err != nil {
		log.Fatal("Failed to create dest dir:", pathToFile)
	}
	err = os.Rename(filepath.Join(origDir, relativePath), filepath.Join(destDir, relativePath))
	if err != nil {
		log.Fatal("Failed to move file:", relativePath)
	}
}

func ReadLinesFromFile(filePath string) []string {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	lines := make([]string, 0)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines
}

// Returns files to be removed.
func DedupFiles(dupFiles []string, dirOrder []string) []string {
	filesToRemove := []string{}
	if len(dirOrder) > 0 {
		fileIdxToKeep := make(map[int]int)
		for _, dir := range dirOrder {
			for index, file := range dupFiles {
				if strings.HasPrefix(file, dir) {
					fileIdxToKeep[index] = 1
				}
			}
			if len(fileIdxToKeep) > 0 {
				break
			}
		}
		if len(fileIdxToKeep) > 0 {
			for index, file := range dupFiles {
				if fileIdxToKeep[index] == 0 {
					filesToRemove = append(filesToRemove, file)
				}
			}
			if len(fileIdxToKeep) == 1 {
				return filesToRemove
			}

			remainFiles := []string{}
			for idx, _ := range fileIdxToKeep {
				remainFiles = append(remainFiles, dupFiles[idx])
			}
			dupFiles = remainFiles
		}
	}

	sort.Sort(sort.StringSlice(dupFiles))
	for _, file := range dupFiles[1:] {
		filesToRemove = append(filesToRemove, file)
	}
	return filesToRemove
}
