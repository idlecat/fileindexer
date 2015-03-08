package main

import (
	"flag"
	"fmt"
	"github.com/idlecat/fileindexer"
)

var baseDir = flag.String("baseDir", "", "dir to open index")
var dedupDir = flag.String("dedupDir", "", "dir to check duplicated files")

func main() {
	flag.Parse()
	fmt.Println("Hello world!", *baseDir, *dedupDir)
	for i := 0; i < flag.NArg(); i++ {
		fmt.Println(flag.Arg(i))
	}

	indexer := fileindexer.NewIndexer(*baseDir)
	defer indexer.Close()

	meta, err := indexer.GetFileMeta(flag.Arg(0))
	if err != nil {
		fmt.Printf("Error when checking %s\n", flag.Arg(0))
	} else {
		fmt.Println(meta)
	}
}
