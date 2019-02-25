package tools

import (
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"github.com/klauspost/reedsolomon"
)

const (
	REBUILD    = 3 // 重建时，重试次数
	SHARDERROR = 2 // shard切片容忍误差: 2byte
)

// 需要注意的问题是： 数据块大小不一致时(不是数据缺失)，还是能还原，但是还原出来的数据也是损坏的
// 所以，需要检查每一个切片的size，并找出size异常的文件，修复他们

// 实现切分和还原，不管是校验块还是数据块，其大小都是一致的
type rsFile struct {
	dataCount   int    // 数据分片数目
	parityCount int    // 校验块数目
	srcPath     string // 源文件/path/to/file.jpg
	fileName    string // 文件名
	destBaseDir string // 切片的目标目录，其文件格式为：/tmp/dss/
	enc         reedsolomon.StreamEncoder
}

// srcPath: 源文件
// destBaseDir: 目标基础目录
func (rs *rsFile) RSSplit() ([]string, error) {
	var (
		shardName    string // 切片名
		src          *os.File
		size         int64
		shardNameArr = make([]string, rs.dataCount+rs.parityCount)
		fileArr      = make([]*os.File, rs.dataCount+rs.parityCount)
		err          error
	)
	if src, err = os.Open(rs.srcPath); err != nil {
		return shardNameArr, err
	}
	finfo, _ := src.Stat()
	size = finfo.Size() // 文件大小
	// 初始化所有文件
	for i := 0; i < rs.dataCount+rs.parityCount; i++ {
		shardName = fmt.Sprintf("%s.%d", filepath.Join(rs.destBaseDir, filepath.Base(rs.srcPath)), i)
		shardNameArr[i] = shardName
		DirExist(filepath.Dir(shardName))
		f, _ := os.OpenFile(shardName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
		fileArr[i] = f
		defer f.Close()
	}
	var w1 = make([]io.Writer, rs.dataCount)
	for i := range w1 {
		w1[i] = fileArr[i]
	}
	// 处理data
	if err = rs.enc.Split(src, w1, size); err != nil {
		return shardNameArr, err
	}
	// 重置data切片的偏移量
	for i := 0; i < rs.dataCount; i++ {
		fileArr[i].Seek(0, 0)
	}
	// 生成校验块
	var (
		w = make([]io.Writer, rs.parityCount)
		r = make([]io.Reader, rs.dataCount)
	)
	for i := range w {
		w[i] = fileArr[rs.dataCount+i]
	}
	for i := range r {
		r[i] = fileArr[i]
	}
	if err = rs.enc.Encode(r, w); err != nil {
		return shardNameArr, err
	}
	return shardNameArr, nil
}

// 还原数据，发生错误时，重建数据
// rebuildCount 重建次数
func (rs *rsFile) RSReBuild(count int) error {
	var (
		reader []io.Reader
		writer []io.Writer
		err    error
		ok     bool
	)
	reader, _ = rs.generateReader()
	for i := 0; i < count; i++ {
		ok, err = rs.enc.Verify(reader)
		if ok {
			return nil
		}
		reader, _ = rs.generateReader()
		writer = rs.generateWriter(reader)
		err = rs.enc.Reconstruct(reader, writer)
	}
	return err
}

// 生成文件,当rebuild为0时，表示不修复
func (rs *rsFile) GenerateFile(savefile string, isRebuild bool) error {
	var (
		f            *os.File
		reader, size = rs.generateReader()
		err          error
	)
	if len(savefile) == 0 {
		f, _ = os.Create(rs.fileName)
	} else {
		f, _ = os.Create(savefile)
	}
	err = rs.enc.Join(f, reader, size*int64((rs.dataCount)))
	if err == nil {
		return nil
	}
	if !isRebuild {
		return err
	}
	log.Println("无法导出文件，正在尝试修复分片")
	err = rs.RSReBuild(REBUILD)
	if err == nil {
		log.Println("分片修复成功")
	}
	reader, size = rs.generateReader()
	return rs.enc.Join(f, reader, size*int64((rs.dataCount)))
}

// 返回分片的reader数组
func (rs *rsFile) generateReader() ([]io.Reader, int64) {
	var (
		maxSize, s int64                                        // 最大的size文件大小
		sizeArr    = make([]int64, rs.dataCount+rs.parityCount) // 存放切片的大小
		shards     = make([]io.Reader, rs.dataCount+rs.parityCount)
		fileBase   = filepath.Join(rs.destBaseDir, rs.fileName) + ".%d" // 文件路径
		fileName   string
		f          *os.File
		err        error
		stat       os.FileInfo
	)
	for i := range shards {
		fileName = fmt.Sprintf(fileBase, i)
		f, err = os.Open(fileName)
		if err != nil {
			shards[i] = nil
			continue
		} else {
			shards[i] = f
		}
		stat, err = f.Stat()
		s = stat.Size()
		if s > maxSize {
			maxSize = s
		}
		sizeArr[i] = s
	}
	for i := 0; i < (rs.dataCount + rs.parityCount); i++ {
		if sizeArr[i] == 0 || (sizeArr[i]+SHARDERROR) < maxSize {
			shards[i] = nil
		}
	}
	return shards, maxSize
}

// 生成io.writer，为nil空时才创建
func (rs *rsFile) generateWriter(reader []io.Reader) []io.Writer {
	var (
		fileBase = filepath.Join(rs.destBaseDir, rs.fileName) + ".%d" // 文件路径
		fileName string
		f        *os.File
		w        = make([]io.Writer, len(reader))
	)
	for i := range w {
		if reader[i] != nil {
			continue
		}
		fileName = fmt.Sprintf(fileBase, i)
		f, _ = os.Create(fileName)
		w[i] = f
	}
	return w
}

// filePath为单个文件
// destDir为目录名
func NewrsFile(filePath, destDir string, dataCount, parityCount int) *rsFile {
	var enc, _ = reedsolomon.NewStream(dataCount, parityCount)
	return &rsFile{
		dataCount:   dataCount,
		parityCount: parityCount,
		srcPath:     filePath,
		destBaseDir: destDir,
		fileName:    filepath.Base(filePath),
		enc:         enc,
	}
}
