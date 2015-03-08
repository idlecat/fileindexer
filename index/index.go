package main

import (
	"flag"
	"github.com/idlecat/fileindexer"
)

var baseDir = flag.String("baseDir", "", "dir to open index")

func main() {
	flag.Parse()
	indexer := fileindexer.NewIndexer(*baseDir)
	indexer.Update()
	defer indexer.Close()
}
