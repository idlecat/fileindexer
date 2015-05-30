package main

import (
	"flag"
	"fmt"
	"github.com/idlecat/fileindexer"
	"github.com/idlecat/fileindexer/protos"
	"os"
)

var baseDir = flag.String("baseDir", "", "dir to open index")
var op = flag.String("op", "info", "operations defined as OP_*")
var dedupDir = flag.String("dedupDir", "", "dir to check duplicated files")

const (
	OP_UPDATE = "update"
	OP_INFO = "info"
	OP_LIST = "list"
	OP_DEDUP = "dedup"
)
var indexer *fileindexer.Indexer

func main() {
	flag.Parse()
	var err error
	if *baseDir == "" {
		*baseDir, err = os.Getwd()
		if err != nil {
			fmt.Println("baseDir should be specified.")
		}
	}
	for i := 0; i < flag.NArg(); i++ {
		fmt.Println(flag.Arg(i))
	}
	indexer = fileindexer.NewIndexer(*baseDir)
	defer indexer.Close()

	switch *op {
		case OP_UPDATE:
			update()
		case OP_INFO:
			info()
		case OP_LIST:
			list()
		case OP_DEDUP:
			dedup()
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
	fmt.Println("baseDir:", indexer.GetDbMeta().BaseDir)
}

func list() {
	indexer.Iter(func(file string, meta *protos.FileMeta) {
		fmt.Println(file, meta)
	})
}

func dedup() {
	
}
