package main

import (
	"flag"
	"fmt"
	"github.com/idlecat/fileindexer"
	"github.com/idlecat/fileindexer/protos"
	"log"
)

var baseDir = flag.String("baseDir", "", "dir to open index")
var op = flag.String("op", "info", "operations defined as OP_*")
var dedupDir = flag.String("dedupDir", "", "dir to check duplicated files")

const (
	OP_UPDATE    = "update"
	OP_INFO      = "info"
	OP_LIST      = "list"
	OP_DEDUP     = "dedup"
	OP_QUICKSCAN = "qscan"
)

var indexer *fileindexer.Indexer

func main() {
	flag.Parse()
	if *baseDir == "" {
		log.Fatal("baseDir should be specified.")
	}
	indexer = fileindexer.NewIndexer(*baseDir)
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
	}

	meta := indexer.GetFileMeta(flag.Arg(0), false)
	if meta != nil {
		fmt.Println(meta)
	}
}

func update() {
	indexer.Update()
}

func info() {
	fmt.Println(indexer.GetDbMeta())
	fmt.Println(indexer.GetFileMeta("", true))
}

func list() {
	indexer.Iter(func(file string, meta *protos.FileMeta) {
		fmt.Println(file, meta)
	})
}

func dedup() {

}

func quickScan() {
	info := fileindexer.RepositoryInfo{}
	indexer.QuickScan(&info)
	fmt.Printf("Total File:%d, Total Size:%d\n", info.FileCount, info.FileSize)
}
