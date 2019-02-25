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
)

// 所有逻辑处理，与es交互

const (
	ELASTIC_URL   = "http://192.168.10.150:9200"
	ES_INDEX      = "objstorage" // 索引名，所有服务共用一个索引
	ES_TYPE_SHARD = "shard"
)

var (
	BaseDir      = "/data1"
	TmpDir       = "/data1/tmp"
	FileToken    = make(map[string]*Files)
	TokenTimeOut = 5 * time.Second // token有效时间
	FileMU       = &sync.RWMutex{}
	ESearch      = tools.NewES(ELASTIC_URL, ES_INDEX) // ES实例
	RunningMap   = map[string]struct{}{}              // 保存正在执行保存任务的md5，以免重复上传分片
	RunningMU    = &sync.RWMutex{}                    // 保护RunningMap
)

var (
	ErrMD5    = errors.New("MD5有误")
	ErrUpload = errors.New("上传文件时出错")
	ErrES     = errors.New("ES中不存在Doc")
	Err405    = errors.New("非法Method")
	Err500    = errors.New("500 Server Error")
	Err404    = errors.New("404 Not Found")
	Err403    = errors.New("403 Forbidden")
)

type Files struct {
	ServerPath string
	ReqTime    time.Time
}

func init() {
	var (
		f   os.FileInfo
		err error
	)
	if l := os.Getenv("LISTEN_ADDR"); len(l) != 0 {
		ListenAddr = l
	}
	if tmp := os.Getenv("BaseDir"); len(tmp) != 0 {
		BaseDir = tmp
	}
	Dir = filepath.Join(BaseDir, strings.Replace(ListenAddr, ":", ".", -1))
	if f, err = os.Stat(Dir); err != nil {
		if os.IsNotExist(err) {
			log.Println("创建新的分片目录: ", Dir)
			os.MkdirAll(Dir, 0755)
			return
		}
	}
	if f.IsDir() {
		log.Println("已存在分片目录: ", Dir)
	} else {
		log.Fatalln("存储分片类型须为目录: ", Dir)
	}
}

// 数据切片，通过ES获取
type Shard struct {
	Size    int64  `json:"size"` // 切片文件大小
	Create  int64  `json:"create"`
	MD5     string `json:"md5"`
	SerPath string `json:"ser_path"` // 存放在data中的路径，如xxx/shards.xx，一般为md5.01这类名
	Server  string `json:"server"`   // 存放在哪个区服, 例如: 0.0.0.0:8000
}

// 检验md5并返回跳转url
func (s *Shard) CheckShard(resp http.ResponseWriter, req *http.Request) {
	var (
		token   string // 临时token
		serpath string
	)
	s.MD5 = req.FormValue(P_MD5)
	if len(s.MD5) != 32 || !ESearch.IsExists(ES_TYPE_SHARD, s.MD5) {
		log.Println("ES中不存在分片信息: ", s.MD5)
		resp.Write(tools.Json2Byte(500, ErrMD5.Error()))
		return
	}
	res, _ := ESearch.GetOne(ES_TYPE_SHARD, s.MD5)
	if err := json.Unmarshal(*res.Source, s); err != nil {
		log.Println(err.Error())
		resp.Write(tools.Json2Byte(500, Err500.Error()))
		return
	}

	serpath = filepath.Join(Dir, s.SerPath)
	if !tools.FileExist(serpath) {
		log.Println("不存在该shard: ", serpath)
		resp.Write(tools.Json2Byte(404, Err404.Error()))
		return
	}
	if !tools.MD5Diff(s.MD5, serpath) {
		resp.Write(tools.Json2Byte(500, ErrMD5.Error()))
		return
	}
	token = tools.RandomString(8)
	FileMU.Lock()
	FileToken[token] = &Files{
		ServerPath: serpath,
		ReqTime:    time.Now(),
	}
	FileMU.Unlock()
	resp.Write(tools.Json2Byte(302, token))
}

// 删除切片
func (s *Shard) DeleteShard(resp http.ResponseWriter, req *http.Request) {
	// 处理md5
	s.MD5 = req.FormValue(P_MD5)
	if 32 != len(s.MD5) {
		resp.Write(tools.Json2Byte(500, ErrMD5.Error()))
		return
	}
	if ESearch.IsExists(ES_TYPE_SHARD, s.MD5) {
		log.Println("从ES中删除切片文档: ", s.MD5)
		if _, err := ESearch.Delete(ES_TYPE_SHARD, s.MD5); err != nil {
			resp.Write(tools.Json2Byte(500, Err500.Error()))
			return
		}
	} else {
		log.Println("ES不存在切片文档: ", s.MD5)
	}

	// 处理分片
	s.SerPath = req.FormValue(P_PATH)
	tmpfile := filepath.Join(Dir, s.SerPath)
	if tools.FileExist(tmpfile) {
		log.Println("删除切片文件: ", tmpfile)
		os.Remove(tmpfile)
	} else {
		log.Println("不存在切片文件: ", tmpfile)
	}
	resp.Write(tools.Json2Byte(200, "成功删除切片: "+s.SerPath))
}

// 发送文件，直接通过resp
func (s *Shard) SendShard(resp http.ResponseWriter, req *http.Request) {
	var (
		ok    bool
		token string // 传递过来的token
		f     *Files
	)

	// token有效性
	token = req.FormValue(P_TOKEN)
	if len(token) != 8 {
		resp.Write(tools.Json2Byte(403, Err403.Error()))
		return
	}
	FileMU.RLock()
	f, ok = FileToken[token]
	FileMU.RUnlock()
	if !ok || time.Now().Sub(f.ReqTime) > TokenTimeOut {
		log.Println("无效token: ", token)
		resp.Write(tools.Json2Byte(404, Err404.Error()))
		return
	}

	// ServerPath路径查找文件
	if tools.FileExist(f.ServerPath) {
		log.Println("发送切片: ", f.ServerPath)
		http.ServeFile(resp, req, f.ServerPath)
	} else {
		log.Println("不存在该切片: ", f.ServerPath)
		resp.Write(tools.Json2Byte(404, Err404.Error()))
	}
	FileMU.Lock()
	delete(FileToken, token)
	FileMU.Unlock()
}

// 获取put新建的文件，需要检验md5，提交至ES索引
func (s *Shard) ShardServer(resp http.ResponseWriter, req *http.Request) {
	var (
		serpath, tmp string // 绝对路径
		err          error
		f            *os.File
		pf           multipart.File // form文件
		finfo        os.FileInfo
	)
	// 判断MD5
	s.MD5 = req.FormValue(P_MD5)
	if len(s.MD5) != 32 {
		log.Println("MD5长度有误: ", s.MD5)
		resp.Write(tools.Json2Byte(400, ErrMD5.Error()))
		return
	}
	RunningMU.RLock()
	_, ok := RunningMap[s.MD5]
	RunningMU.RUnlock()
	if ok {
		log.Println("文件在复制队列: ", s.SerPath)
		resp.Write(tools.Json2Byte(500, Err500.Error()))
		return
	}

	// 加锁
	RunningMU.Lock()
	RunningMap[s.MD5] = struct{}{}
	RunningMU.Unlock()
	defer func() {
		RunningMU.Lock()
		delete(RunningMap, s.MD5) // 去掉运行锁
		log.Println("RuningMap中删除Key: ", s.MD5)
		RunningMU.Unlock()
	}()

	// 判断文件
	s.SerPath = req.FormValue(P_PATH)
	serpath = filepath.Join(Dir, s.SerPath)
	tmp = filepath.Join(TmpDir, s.SerPath)
	if tools.FileExist(serpath) {
		log.Println("已存在,将覆盖该文件: ", serpath)
	}
	os.MkdirAll(filepath.Dir(tmp), 0755)
	f, _ = os.Create(tmp) // 创建临时文件
	defer func() {
		f.Close()
		os.Remove(tmp)
	}()

	// 开始获取postform文件，复制文件进相关目录
	if pf, _, err = req.FormFile(P_FILE); err != nil {
		log.Println("获取form文件时出错: ", err.Error())
		resp.Write(tools.Json2Byte(500, Err500.Error()))
		return
	}

	log.Println("存进临时文件: ", tmp)
	if !tools.MD5AndStorage(pf, f, s.MD5) {
		log.Println("MD5不一致")
		resp.Write(tools.Json2Byte(400, ErrMD5.Error()))
		return
	}

	// 上传至ES数据库
	finfo, _ = os.Stat(tmp)
	s.Size = finfo.Size()
	if _, err = ESearch.Add(ES_TYPE_SHARD, s.MD5, s); err != nil {
		log.Println("提交到ES时出错: ", err.Error())
		resp.Write(tools.Json2Byte(500, Err500.Error()))
		return
	}
	os.MkdirAll(filepath.Dir(serpath), 0755)
	if err = tools.MoveFile(tmp, serpath); err != nil {
		log.Println(err.Error())
		resp.Write(tools.Json2Byte(500, Err500.Error()))
	} else {
		log.Println("success,成功存进: ", serpath)
		resp.Write(tools.Json2Byte(200, "成功存进Data: "+s.SerPath))
	}
}
