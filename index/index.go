package main

import (
	"flag"
	"fmt"
	"github.com/idlecat/fileindexer"
	"os"
)

var baseDir = flag.String("baseDir", "", "dir to open index")

func main() {
	var err error
	flag.Parse()
	if *baseDir == "" {
		*baseDir, err = os.Getwd()
		if err != nil {
			fmt.Println("baseDir should be specified.")
		}
	}
	indexer := fileindexer.NewIndexer(*baseDir)
	indexer.Update()
	defer indexer.Close()
}
