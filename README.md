Fileindxer

I wrote this program to get rid of all duplicated photo backups. It's my first
golang project so bear with me. I wrote tests for major logic and duplicated
files are not actually removed but move to temp directory.

The simplest usage is:

1. Move all files to one directory, say AllFilesDir
2. Build index
$ go run indexer_cmd/index.go --baseDir=AllFilesDir --op=update
3. Dedup (dryrun) to list all files that are going to be deduped.
$ go run indexer_cmd/index.go --baseDir=AllFilesDir \
     --op=dedup --tmpDir=/tmp/tmpDir --dirOrder=/tmp/dirOrder.txt --dryRun=true
4. Dedup for real
$ go run indexer_cmd/index.go --baseDir=AllFilesDir \
     --op=dedup --tmpDir=/tmp/tmpDir --dirOrder=/tmp/dirOrder.txt --dryRun=false


A few tech details:
1. An index based on leveldb is built for the file directory. The db contains
   following entries:
  path -> FileMeta
  file_hash -> FilePaths
2. Protobuf is used.
