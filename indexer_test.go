package fileindexer_test

import (
	"github.com/idlecat/fileindexer"
	"github.com/idlecat/fileindexer/protos"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

func FatalErr(err error, msg string) {
	if err != nil {
		log.Fatal(msg, err)
	}
}

func setUp() string {
	dir, err := ioutil.TempDir("", "fileindexer")
	FatalErr(err, "")
	dir1 := filepath.Join(dir, "dir1")
	_ = os.Mkdir(dir1, 0777)
	_ = ioutil.WriteFile(filepath.Join(dir1, "abc"), []byte("abc"), 0666)

	dir2 := filepath.Join(dir, "dir2")
	_ = os.Mkdir(dir2, 0777)
	_ = ioutil.WriteFile(filepath.Join(dir2, "xyz"), []byte("xyz"), 0666)

	dir11 := filepath.Join(dir1, "dir11")
	_ = os.Mkdir(dir11, 0777)
	_ = ioutil.WriteFile(filepath.Join(dir11, "xdong"), []byte("xdong"), 0666)
	return dir
}

type DirTest struct {
	relativePath string
	dirInfo      protos.DirInfo
}

type FileTest struct {
	relativePath string
	meta         protos.FileMeta
}

type HashTest struct {
	hash  string
	files []string
}

func VerifyDirTests(indexer *fileindexer.Indexer, tests []DirTest, t *testing.T) {
	for _, dt := range tests {
		meta := indexer.GetFileOrDirMeta(dt.relativePath)
		if meta == nil {
			t.Errorf("%s has no meta found", dt.relativePath)
			continue
		}
		if !meta.IsDir {
			t.Errorf("%s is file", dt.relativePath)
			continue
		}
		ExpectEqual(t, dt.dirInfo.TotalFileSize, meta.DirInfo.TotalFileSize, dt.relativePath+" file size")
		ExpectEqual(t, dt.dirInfo.TotalFileCount, meta.DirInfo.TotalFileCount, dt.relativePath+" file count")
	}
}

func VerifyFileTests(indexer *fileindexer.Indexer, tests []FileTest, t *testing.T) {
	for _, ft := range tests {
		meta := indexer.GetFileOrDirMeta(ft.relativePath)
		if meta == nil {
			t.Errorf("%s has no meta found", ft.relativePath)
			continue
		}
		if meta.IsDir {
			t.Errorf("%s is directory", ft.relativePath)
			continue
		}
		ExpectEqual(t, ft.meta.Size, meta.Size, ft.relativePath+" size")
		ExpectEqual(t, ft.meta.Md5Sum, meta.Md5Sum, ft.relativePath+" md5sum")
	}
}

func VerifyHashTests(indexer *fileindexer.Indexer, tests []HashTest, t *testing.T) {
	for _, test := range tests {
		_, files := indexer.GetFilesByHash(test.hash)
		if files == nil && test.files != nil {
			t.Errorf("hash %s has no file found", test.hash)
			continue
		}
		ExpectSliceEqual(t, test.files, files, test.hash)
	}
}

func ExpectEqual(t *testing.T, expected interface{}, actual interface{}, name string) {
	if expected != actual {
		t.Errorf("%s: expected %d, actual %d", name, expected, actual)
	}
}

func ExpectSliceEqual(t *testing.T, expected []string, actual []string, name string) {
	if len(expected) != len(actual) {
		t.Errorf("%s size mismatch", name)
		return
	}
	expectedMap := make(map[string]int)
	// assume no duplication
	for _, v := range expected {
		expectedMap[v] = 1
	}

	for _, v := range actual {
		if expectedMap[v] == 0 {
			t.Errorf("%s: %s from actual not found in expected", name, v)
		}
	}
}

const (
	ABC_MD5SUM   = "900150983cd24fb0d6963f7d28e17f72"
	XYZ_MD5SUM   = "d16fb36f0911f878998c136191af705e"
	XDONG_MD5SUM = "ac970faf8b99258047c5a385070e3d64"
)

func TestCreateIndexer(t *testing.T) {
	dir := setUp()
	defer os.RemoveAll(dir)

	indexer := fileindexer.OpenOrCreate(dir, "")
	defer indexer.Close()
	indexer.Update()

	dbMeta := indexer.GetDbMeta()
	if dbMeta.BaseDir != dir || dbMeta.Sequence != 1 {
		t.Errorf("DbMeta", dbMeta)
	}

	dirTests := []DirTest{
		{"", protos.DirInfo{TotalFileSize: 11, TotalFileCount: 3}},
		{"dir1", protos.DirInfo{TotalFileSize: 8, TotalFileCount: 2}},
		{"dir1/dir11", protos.DirInfo{TotalFileSize: 5, TotalFileCount: 1}},
		{"dir2", protos.DirInfo{TotalFileSize: 3, TotalFileCount: 1}},
	}
	VerifyDirTests(indexer, dirTests, t)

	fileTests := []FileTest{
		{"dir1/abc", protos.FileMeta{Size: 3, Md5Sum: ABC_MD5SUM}},
		{"dir2/xyz", protos.FileMeta{Size: 3, Md5Sum: XYZ_MD5SUM}},
		{"dir1/dir11/xdong", protos.FileMeta{Size: 5, Md5Sum: XDONG_MD5SUM}},
	}
	VerifyFileTests(indexer, fileTests, t)

	hashTests := []HashTest{
		{ABC_MD5SUM, []string{"dir1/abc"}},
		{XYZ_MD5SUM, []string{"dir2/xyz"}},
		{XDONG_MD5SUM, []string{"dir1/dir11/xdong"}},
	}
	VerifyHashTests(indexer, hashTests, t)
}

func TestUpdateIndexerWithoutChange(t *testing.T) {
	dir := setUp()
	defer os.RemoveAll(dir)

	indexer := fileindexer.OpenOrCreate(dir, "")
	defer indexer.Close()
	indexer.Update()
	indexer.Update()

	dbMeta := indexer.GetDbMeta()
	if dbMeta.BaseDir != dir || dbMeta.Sequence != 2 {
		t.Errorf("DbMeta", dbMeta)
	}

	dirTests := []DirTest{
		{"", protos.DirInfo{TotalFileSize: 11, TotalFileCount: 3}},
		{"dir1", protos.DirInfo{TotalFileSize: 8, TotalFileCount: 2}},
		{"dir1/dir11", protos.DirInfo{TotalFileSize: 5, TotalFileCount: 1}},
		{"dir2", protos.DirInfo{TotalFileSize: 3, TotalFileCount: 1}},
	}
	VerifyDirTests(indexer, dirTests, t)

	fileTests := []FileTest{
		{"dir1/abc", protos.FileMeta{Size: 3, Md5Sum: ABC_MD5SUM}},
		{"dir2/xyz", protos.FileMeta{Size: 3, Md5Sum: XYZ_MD5SUM}},
		{"dir1/dir11/xdong", protos.FileMeta{Size: 5, Md5Sum: XDONG_MD5SUM}},
	}
	VerifyFileTests(indexer, fileTests, t)

	hashTests := []HashTest{
		{ABC_MD5SUM, []string{"dir1/abc"}},
		{XYZ_MD5SUM, []string{"dir2/xyz"}},
		{XDONG_MD5SUM, []string{"dir1/dir11/xdong"}},
	}
	VerifyHashTests(indexer, hashTests, t)
}

func TestFileRemoved(t *testing.T) {
	dir := setUp()
	defer os.RemoveAll(dir)

	indexer := fileindexer.OpenOrCreate(dir, "")
	defer indexer.Close()
	indexer.Update()

	_ = os.Remove(filepath.Join(dir, "dir1/abc"))
	indexer.Update()

	dirTests := []DirTest{
		{"", protos.DirInfo{TotalFileSize: 8, TotalFileCount: 2}},
		{"dir1", protos.DirInfo{TotalFileSize: 5, TotalFileCount: 1}},
		{"dir1/dir11", protos.DirInfo{TotalFileSize: 5, TotalFileCount: 1}},
		{"dir2", protos.DirInfo{TotalFileSize: 3, TotalFileCount: 1}},
	}
	VerifyDirTests(indexer, dirTests, t)

	fileTests := []FileTest{
		{"dir2/xyz", protos.FileMeta{Size: 3, Md5Sum: XYZ_MD5SUM}},
		{"dir1/dir11/xdong", protos.FileMeta{Size: 5, Md5Sum: XDONG_MD5SUM}},
	}
	VerifyFileTests(indexer, fileTests, t)

	hashTests := []HashTest{
		{ABC_MD5SUM, nil},
		{XYZ_MD5SUM, []string{"dir2/xyz"}},
		{XDONG_MD5SUM, []string{"dir1/dir11/xdong"}},
	}
	VerifyHashTests(indexer, hashTests, t)
}

func TestDirRemoved(t *testing.T) {
	dir := setUp()
	defer os.RemoveAll(dir)

	indexer := fileindexer.OpenOrCreate(dir, "")
	defer indexer.Close()
	indexer.Update()

	_ = os.RemoveAll(filepath.Join(dir, "dir1/dir11"))
	indexer.Update()

	dirTests := []DirTest{
		{"", protos.DirInfo{TotalFileSize: 6, TotalFileCount: 2}},
		{"dir1", protos.DirInfo{TotalFileSize: 3, TotalFileCount: 1}},
		{"dir2", protos.DirInfo{TotalFileSize: 3, TotalFileCount: 1}},
	}
	VerifyDirTests(indexer, dirTests, t)

	fileTests := []FileTest{
		{"dir1/abc", protos.FileMeta{Size: 3, Md5Sum: ABC_MD5SUM}},
		{"dir2/xyz", protos.FileMeta{Size: 3, Md5Sum: XYZ_MD5SUM}},
	}
	VerifyFileTests(indexer, fileTests, t)

	hashTests := []HashTest{
		{ABC_MD5SUM, []string{"dir1/abc"}},
		{XYZ_MD5SUM, []string{"dir2/xyz"}},
		{XDONG_MD5SUM, nil},
	}
	VerifyHashTests(indexer, hashTests, t)
}

func TestFileModified(t *testing.T) {
	dir := setUp()
	defer os.RemoveAll(dir)

	indexer := fileindexer.OpenOrCreate(dir, "")
	defer indexer.Close()
	indexer.Update()

	_ = ioutil.WriteFile(filepath.Join(dir, "dir1/abc"), []byte("xdong"), 0666)
	indexer.Update()

	dirTests := []DirTest{
		{"", protos.DirInfo{TotalFileSize: 13, TotalFileCount: 3}},
		{"dir1", protos.DirInfo{TotalFileSize: 10, TotalFileCount: 2}},
		{"dir1/dir11", protos.DirInfo{TotalFileSize: 5, TotalFileCount: 1}},
		{"dir2", protos.DirInfo{TotalFileSize: 3, TotalFileCount: 1}},
	}
	VerifyDirTests(indexer, dirTests, t)

	fileTests := []FileTest{
		{"dir1/abc", protos.FileMeta{Size: 5, Md5Sum: XDONG_MD5SUM}},
		{"dir2/xyz", protos.FileMeta{Size: 3, Md5Sum: XYZ_MD5SUM}},
		{"dir1/dir11/xdong", protos.FileMeta{Size: 5, Md5Sum: XDONG_MD5SUM}},
	}
	VerifyFileTests(indexer, fileTests, t)

	hashTests := []HashTest{
		{ABC_MD5SUM, nil},
		{XYZ_MD5SUM, []string{"dir2/xyz"}},
		{XDONG_MD5SUM, []string{"dir1/dir11/xdong", "dir1/abc"}},
	}
	VerifyHashTests(indexer, hashTests, t)
}

func TestFileAdded(t *testing.T) {
	dir := setUp()
	defer os.RemoveAll(dir)

	indexer := fileindexer.OpenOrCreate(dir, "")
	defer indexer.Close()
	indexer.Update()

	_ = ioutil.WriteFile(filepath.Join(dir, "dir1/xdong"), []byte("xdong"), 0666)
	indexer.Update()

	dirTests := []DirTest{
		{"", protos.DirInfo{TotalFileSize: 16, TotalFileCount: 4}},
		{"dir1", protos.DirInfo{TotalFileSize: 13, TotalFileCount: 3}},
		{"dir1/dir11", protos.DirInfo{TotalFileSize: 5, TotalFileCount: 1}},
		{"dir2", protos.DirInfo{TotalFileSize: 3, TotalFileCount: 1}},
	}
	VerifyDirTests(indexer, dirTests, t)

	fileTests := []FileTest{
		{"dir1/abc", protos.FileMeta{Size: 3, Md5Sum: ABC_MD5SUM}},
		{"dir1/xdong", protos.FileMeta{Size: 5, Md5Sum: XDONG_MD5SUM}},
		{"dir2/xyz", protos.FileMeta{Size: 3, Md5Sum: XYZ_MD5SUM}},
		{"dir1/dir11/xdong", protos.FileMeta{Size: 5, Md5Sum: XDONG_MD5SUM}},
	}
	VerifyFileTests(indexer, fileTests, t)

	hashTests := []HashTest{
		{ABC_MD5SUM, []string{"dir1/abc"}},
		{XYZ_MD5SUM, []string{"dir2/xyz"}},
		{XDONG_MD5SUM, []string{"dir1/dir11/xdong", "dir1/xdong"}},
	}
	VerifyHashTests(indexer, hashTests, t)
}

func TestDirAdded(t *testing.T) {
	dir := setUp()
	defer os.RemoveAll(dir)

	indexer := fileindexer.OpenOrCreate(dir, "")
	defer indexer.Close()
	indexer.Update()

	_ = os.Mkdir(filepath.Join(dir, "dir1/dir12"), 0777)
	_ = ioutil.WriteFile(filepath.Join(dir, "dir1/dir12/abc"), []byte("abc"), 0666)
	indexer.Update()

	dirTests := []DirTest{
		{"", protos.DirInfo{TotalFileSize: 14, TotalFileCount: 4}},
		{"dir1", protos.DirInfo{TotalFileSize: 11, TotalFileCount: 3}},
		{"dir1/dir11", protos.DirInfo{TotalFileSize: 5, TotalFileCount: 1}},
		{"dir1/dir12", protos.DirInfo{TotalFileSize: 3, TotalFileCount: 1}},
		{"dir2", protos.DirInfo{TotalFileSize: 3, TotalFileCount: 1}},
	}
	VerifyDirTests(indexer, dirTests, t)

	fileTests := []FileTest{
		{"dir1/abc", protos.FileMeta{Size: 3, Md5Sum: ABC_MD5SUM}},
		{"dir2/xyz", protos.FileMeta{Size: 3, Md5Sum: XYZ_MD5SUM}},
		{"dir1/dir11/xdong", protos.FileMeta{Size: 5, Md5Sum: XDONG_MD5SUM}},
		{"dir1/dir12/abc", protos.FileMeta{Size: 3, Md5Sum: ABC_MD5SUM}},
	}
	VerifyFileTests(indexer, fileTests, t)

	hashTests := []HashTest{
		{ABC_MD5SUM, []string{"dir1/abc", "dir1/dir12/abc"}},
		{XYZ_MD5SUM, []string{"dir2/xyz"}},
		{XDONG_MD5SUM, []string{"dir1/dir11/xdong"}},
	}
	VerifyHashTests(indexer, hashTests, t)
}

type DedupTest struct {
	name     string
	dupFiles []string
	dirOrder []string
	result   []string
}

func VerifyDedupTest(tests []DedupTest, t *testing.T) {
	for _, ti := range tests {
		actual := fileindexer.DedupFiles(ti.dupFiles, ti.dirOrder)
		sort.Sort(sort.StringSlice(actual))
		ExpectSliceEqual(t, ti.result, actual, ti.name)
	}
}

func TestDedupFiles(t *testing.T) {
	dupFiles := []string{"dir1/abc", "dir2/xyz", "dir1/ab"}
	tests := []DedupTest{
		{"EmptyDirOrder", dupFiles, []string{}, []string{"dir1/abc", "dir2/xyz"}},
		{"NonExistDirOrder", dupFiles, []string{"dir3/"}, []string{"dir1/abc", "dir2/xyz"}},
		{"DirOrder", dupFiles, []string{"dir2/"}, []string{"dir1/ab", "dir1/abc"}},
	}
	VerifyDedupTest(tests, t)
}
