package main

import (
	"flag"
	"fmt"
	"github.com/idlecat/fileindexer"
	"github.com/idlecat/fileindexer/protos"
	"log"
	"os"
)

var baseDir = flag.String("baseDir", "", "dir to build index for")
var indexDir = flag.String("indexDir", "", "dir to store index. default to baseDir/fileIndexerDb if provided empty")
var op = flag.String("op", "info", "operations defined as OP_*")
var intersectDir = flag.String("intersectDir", "", "dir to check duplicated files")
var intersectIndexDir = flag.String("intersectIndexDir", "", "index to check duplicated files")

const (
	OP_UPDATE         = "update"
	OP_INFO           = "info"
	OP_LIST           = "list"
	OP_DEDUP          = "dedup"
	OP_QUICKSCAN      = "qscan"
	OP_INTERSECT_WITH = "intersect"
)

var indexer *fileindexer.Indexer

func main() {
	flag.Parse()
	if *baseDir == "" {
		log.Fatal("baseDir should be specified.")
	}
	indexer = fileindexer.OpenOrCreate(*baseDir, *indexDir)
	defer indexer.Close()

	if indexer.GetError() != nil {
		log.Fatal("Failed to create indexer.")
	}

	switch *op {
	case OP_UPDATE:
		update()
	case OP_INFO:
		info()
	case OP_LIST:
		list()
	case OP_DEDUP:
		dedup()
	case OP_QUICKSCAN:
		quickScan()
	case OP_INTERSECT_WITH:
		intersectWith()
	}
}

func update() {
	indexer.Update()
}

func info() {
	fmt.Println(indexer.GetDbMeta())

	meta := indexer.GetFileOrDirMeta(flag.Arg(0))

	if meta != nil {
		fmt.Println(meta)
	} else {
		fmt.Println("No meta found for ", flag.Arg(0))
	}
}

func list() {
	indexer.Iter(func(file string, meta *protos.FileMeta) {
		fmt.Println(file, meta)
	})
}

func quickScan() {
	info := fileindexer.RepositoryInfo{}
	indexer.QuickScan(&info)
	fmt.Printf("Total File:%d, Total Size:%d\n", info.FileCount, info.FileSize)
}

func dedup() {
	count := 0
	var size int64 = 0
	indexer.IterHash(func(hash string, fileSize int64, paths []string) {
		if len(paths) > 1 {
			fmt.Printf("hash:%s\n", hash)
			for _, path := range paths {
				fmt.Println(path)
			}
			count += len(paths) - 1
			size += int64(len(paths)-1) * fileSize
		}
	})
	fmt.Printf("Total: %d\n", count)
}

func intersectWith() {
	if *intersectDir == "" && *intersectIndexDir == "" {
		log.Fatal("Please provide --intersectDir or --intersectIndexDir")
	}
	dupCount := 0
	var dupSize int64 = 0
	uniqCount := 0
	var uniqSize int64 = 0
	if *intersectDir != "" {
		fileindexer.ScanDir(*intersectDir, func(path string, info os.FileInfo) int {
			if info.IsDir() {
				return fileindexer.NORMAL
			}
			hash, _ := fileindexer.HashFile(path)
			_, files := indexer.GetFilesByHash(hash)
			if files != nil && len(files) > 1 {
				// duplicated
				dupCount += 1
				dupSize += info.Size()
			} else {
				uniqCount += 1
				uniqSize += info.Size()
			}
			return fileindexer.NORMAL
		})
	} else {
		otherIndexer := fileindexer.OpenOrDie(*intersectIndexDir)
		otherIndexer.IterHash(func(hash string, fileSize int64, paths []string) {
			_, files := indexer.GetFilesByHash(hash)
			if files != nil && len(files) > 1 {
				// duplicated
				for _, p := range paths {
					fmt.Printf("%s\n", p)
				}
				dupCount += len(files)
				dupSize += fileSize * int64(len(files))
			} else {
				uniqCount += 1
				uniqSize += fileSize * int64(len(files))
			}
		})
	}
	fmt.Printf("Total duplicated files: %d\n", dupCount)
	fmt.Printf("Total duplicated files size: %d\n", dupSize)
	fmt.Printf("Total unique files: %d\n", uniqCount)
	fmt.Printf("Total unique files size: %d\n", uniqSize)
}
