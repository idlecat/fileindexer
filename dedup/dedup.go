package main

import (
	"flag"
	"fmt"
	"github.com/idlecat/fileindexer"
	"os"
)

var baseDir = flag.String("baseDir", "", "dir to open index")
var dedupDir = flag.String("dedupDir", "", "dir to check duplicated files")

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

	indexer := fileindexer.NewIndexer(*baseDir)
	defer indexer.Close()
	fmt.Println("totalFile:", indexer.GetDbMeta())

	meta := indexer.GetFileMeta(flag.Arg(0), false)
	if meta != nil {
		fmt.Println(meta)
	}
}
