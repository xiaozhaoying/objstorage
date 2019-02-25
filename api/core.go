package main

import (
	"encoding/json"
	"errors"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	tools "../tools"
	elastic "gopkg.in/olivere/elastic.v5"
)

// ①上传流程，文件上传至随机文件名，确认好md5后，切片，分片上传至随机data端，最后写ES索引
// ②下载流程，根据提供的md5，生成下载文件，下载给用户
const (
	ELASTIC_URL   = "http://192.168.10.150:9200"
	ES_INDEX      = "objstorage" // 索引名，所有服务共用一个索引
	ES_TYPE_SHARD = "shard"      // 分片类型
	ES_TYPE_FILE  = "file"       // 文件类型
	ES_TYPE_USER  = "user"       // 用户类型
	DATA_C        = 4            // 数据块数目
	PARITY_C      = 2            // 校验块
)

var (
	BaseDir    string                               = "/data1"
	TmpDir     string                               // 存放临时文件目录，一般为:/data/ip.port/
	RunningMap = map[string]struct{}{}              // 保存正在执行任务的md5(全局)
	RunningMU  = &sync.RWMutex{}                    // 保护RunningMap
	ESearch    = tools.NewES(ELASTIC_URL, ES_INDEX) // ES实例
)
var (
	ErrNotFound = errors.New("不存在该文件")
	ErrUpload   = errors.New("上传文件时出错")
)

func init() {
	var (
		tmp string
		err error
		f   os.FileInfo
	)
	if tmp = os.Getenv("TmpDir"); len(tmp) != 0 {
		TmpDir = tmp
	} else {
		TmpDir = filepath.Join(BaseDir, strings.Replace(ListenAddr, ":", ".", -1))
	}
	if f, err = os.Stat(TmpDir); err != nil {
		if os.IsNotExist(err) {
			log.Println("创建新的临时目录: ", TmpDir)
			os.MkdirAll(TmpDir, 0755)
			return
		}
	}
	if f.IsDir() {
		log.Println("已存在临时目录: ", TmpDir)
	} else {
		log.Fatalln("非目录类型: ", TmpDir)
	}
}

// 文件
type ObjFile struct {
	Size     int64      `json:"size"`
	Create   int64      `json:"create"`
	Md5      string     `json:"md5"`
	Name     string     `json:"name"`
	ObjShard []ObjShard `json:"obj_shard"` // 所有分片的md5
}

// 检查元数据信息
func (o *ObjFile) HeadFile(resp http.ResponseWriter, req *http.Request) {
	var (
		err  error
		body []byte
		res  *elastic.GetResult
	)
	if !ESearch.IsExists(ES_TYPE_FILE, o.Md5) {
		log.Println("不存在该文件: ", o.Md5)
		resp.Write(tools.Json2Byte(404, ErrNotFound.Error()))
		return
	}
	if res, err = ESearch.GetOne(ES_TYPE_FILE, o.Md5); err != nil {
		log.Println(err.Error())
		resp.Write(tools.Json2Byte(500, ErrServer500.Error()))
		return
	}
	json.Unmarshal(*res.Source, o)
	if body, err = json.Marshal(o); err != nil {
		log.Println(err.Error())
		resp.Write(tools.Json2Byte(500, ErrServer500.Error()))
		return
	}
	r := struct {
		Code uint16      `json:"code"` // 返回http code响应码
		Msg  interface{} `json:"msg"`
	}{
		Code: 200,
		Msg:  json.RawMessage(body),
	}
	body, _ = json.Marshal(&r)
	resp.Write(body)
}

func (o *ObjFile) DeleteFile(resp http.ResponseWriter, req *http.Request) {
	var (
		sha Sha
		res *elastic.GetResult
		err error
	)
	if !ESearch.IsExists(ES_TYPE_FILE, o.Md5) {
		log.Println("不存在该文件: ", o.Md5)
		resp.Write(tools.Json2Byte(404, ErrNotFound.Error()))
		return
	}
	if res, err = ESearch.GetOne(ES_TYPE_FILE, o.Md5); err != nil {
		log.Println(err.Error())
		resp.Write(tools.Json2Byte(500, ErrServer500.Error()))
		return
	}
	json.Unmarshal(*res.Source, o)
	sha = o.ObjShard
	if err = (&sha).DeleteShard(); err != nil {
		log.Println(err.Error())
		resp.Write(tools.Json2Byte(500, ErrServer500.Error()))
		return
	}
	// 删除ES中file表
	if ESearch.IsExists(ES_TYPE_FILE, o.Md5) {
		if ESearch.Delete(ES_TYPE_FILE, o.Md5); err != nil {
			log.Println(err.Error())
			resp.Write(tools.Json2Byte(500, ErrServer500.Error()))
			return
		}
		log.Println("ES中删除文档: ", o.Md5)
	}
	os.Remove(filepath.Join(TmpDir, o.Name)) // 删除合并提供下载的那个文件
	resp.Write(tools.Json2Byte(200, "成功删除文件: "+o.Md5))
}

func (o *ObjFile) SendFile(resp http.ResponseWriter, req *http.Request) {
	var (
		dest string // 最终合成的文件
		err  error
		res  *elastic.GetResult // ES返回的结果
		sha  Sha
	)
	if !ESearch.IsExists(ES_TYPE_FILE, o.Md5) {
		resp.Write(tools.Json2Byte(404, "不存在该文件: "+o.Md5))
		return
	}
	res, _ = ESearch.GetOne(ES_TYPE_FILE, o.Md5)
	if err = json.Unmarshal(*res.Source, o); err != nil {
		log.Println(err.Error())
		resp.Write(tools.Json2Byte(500, ErrServer500.Error()))
		return
	}
	// 获取所有切片
	sha = o.ObjShard
	if err = (&sha).DownloadShard(); err != nil {
		resp.Write(tools.Json2Byte(500, ErrServer500.Error()))
		return
	}
	// 整合成文件
	dest = filepath.Join(TmpDir, o.Name)
	log.Println("合并文件: ", dest)
	rs := tools.NewrsFile(o.Name, TmpDir, DATA_C, PARITY_C)
	if err = rs.GenerateFile(dest, true); err != nil {
		log.Println(err.Error())
		resp.Write(tools.Json2Byte(500, ErrServer500.Error()))
		return
	}
	http.ServeFile(resp, req, dest)
}

// 获取put新建的文件，与dataserver中的FileServer不同的是，该FileServer需要向nsq消息队列提交自己正在
// 运行的md5，该消息队列相当于全局锁
func (o *ObjFile) FileServer(resp http.ResponseWriter, req *http.Request) error {
	var (
		tmp      = filepath.Join(TmpDir, tools.RandomString(8))
		md5      string
		shardDir string   // 分片数据存放的位置，一般为/tmpdir/md5sum
		shardArr []string // 所有分片的路径全名，用于上传至data server
		err      error
		f        *os.File
		pf       multipart.File // form文件
	)
	o.Create = time.Now().UnixNano()
	o.Name = filepath.Base(tmp)
	//1. 获取postform文件，复制文件进相关目录
	if pf, _, err = req.FormFile(P_FILE); err != nil {
		log.Println("获取form文件时出错: ", err.Error())
		return ErrUpload
	}
	// 复制到临时文件
	tools.DirExist(filepath.Dir(tmp))
	f, _ = os.Create(tmp) // 创建临时文件
	defer func() {
		f.Close()
		os.Remove(tmp)
	}()

	//2. MD5判断，是否已经存在ES和正在运行的队列中
	md5 = tools.Storage(pf, f) // md5判断
	if o.Md5 != md5 {
		log.Println("上传文件MD5不一致: ", o.Md5, md5)
		return ErrUpload
	}
	RunningMU.RLock()
	_, ok := RunningMap[o.Md5]
	RunningMU.RUnlock()
	if ok {
		log.Println("该MD5存在于运行队列: ", o.Md5)
		return ErrUpload
	}
	if ESearch.IsExists(ES_TYPE_FILE, o.Md5) {
		log.Println("ES中已存在该文档: ", o.Md5)
		return ErrUpload
	}

	finfo, _ := f.Stat()
	o.Size = finfo.Size() // 设置大小
	shardDir = filepath.Join(TmpDir, o.Md5)
	tools.DirExist(shardDir)

	// 开始切片
	RunningMU.Lock()
	RunningMap[o.Md5] = struct{}{}
	RunningMU.Unlock()
	defer func() {
		RunningMU.Lock() // 去掉运行锁
		delete(RunningMap, o.Md5)
		log.Println("RuningMap中删除Key: ", o.Md5)
		RunningMU.Unlock()
	}()
	rs := tools.NewrsFile(tmp, shardDir, DATA_C, PARITY_C)
	if shardArr, err = rs.RSSplit(); err != nil { // 切片，并返回所有切片数组
		log.Println("切片时出错: ", err.Error())
		return ErrUpload
	}
	log.Println("success, 切片成功: ", shardDir)

	// 上传切片至data server
	sha := make(Sha, DATA_C+PARITY_C)
	go (&sha).UploadShard(o, shardArr)
	return nil
}
