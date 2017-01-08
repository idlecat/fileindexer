package main

import (
	"flag"
	"fmt"
	"github.com/idlecat/fileindexer"
	"github.com/idlecat/fileindexer/protos"
	"log"
)

var baseDir = flag.String("baseDir", "", "dir to build index for")
var indexDir = flag.String("indexDir", "", "dir to store index. default to baseDir/fileIndexerDb if provided empty")
var op = flag.String("op", "info", "operations defined as OP_*")
var dedupDir = flag.String("dedupDir", "", "dir to check duplicated files")

const (
	OP_UPDATE         = "update"
	OP_INFO           = "info"
	OP_LIST           = "list"
	OP_DEDUP          = "dedup"
	OP_QUICKSCAN      = "qscan"
	OP_INTERSECT_WITH = "intersect_with"
)

var indexer *fileindexer.Indexer

func main() {
	flag.Parse()
	if *baseDir == "" {
		log.Fatal("baseDir should be specified.")
	}
	indexer = fileindexer.NewIndexer(*baseDir, *indexDir)
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

	meta := indexer.GetFileMeta(flag.Arg(0))
	if meta == nil {
		meta = indexer.GetDirMeta(flag.Arg(0))
	}

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
}

func intersectWith() {
}
