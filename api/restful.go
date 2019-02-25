package main

import (
	"errors"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	tools "../tools"
)

const (
	P_TOKEN = "token" // token的名称
	P_MD5   = "md5"
	P_PATH  = "path"
	P_FILE  = "uploadfile" // post上传时表单名字
	P_USER  = "user"
	P_PASS  = "passwd"
)

var (
	ListenAddr   = "192.168.10.150:9000" // 该api服务接听的地址
	ReadTimeout  = 10 * time.Second
	WriteTimeout = 10 * time.Second
)

var (
	ErrMD5 = errors.New("MD5有误")
	Err400 = errors.New("参数有误")
	Err403 = errors.New("403 Forbidden")
	Err405 = errors.New("非法Method")
)

func init() {
	// 获取环境变量
	if l := os.Getenv("ListenAddr"); len(l) != 0 {
		ListenAddr = l
	}
}

type APIServerStruct struct {
	ListenAddr string
	serv       *http.Server // 对外隐藏，提供Start方法替代
}

// 启动程序
func (ds *APIServerStruct) Start() {
	log.Printf("启动API服务: %s\n", ds.serv.Addr)
	log.Fatalln(ds.serv.ListenAndServe())
}

// 关闭程序
func (ds *APIServerStruct) Stop() {}

func NewAPIServer() *APIServerStruct {
	var s = http.NewServeMux()

	// 初始化处理函数
	s.HandleFunc("/file", handlerFile)
	s.HandleFunc("/checkfile", handlerCheckFile)
	s.HandleFunc("/", func(resp http.ResponseWriter, rsq *http.Request) {
		resp.Write(tools.Json2Byte(403, Err403.Error()))
	})

	return &APIServerStruct{
		ListenAddr: ListenAddr,
		serv: &http.Server{
			Addr:           ListenAddr,
			Handler:        s,
			ReadTimeout:    ReadTimeout,
			WriteTimeout:   WriteTimeout,
			MaxHeaderBytes: 1 << 20,
		}}
}

// 处理文件相关功能
func handlerFile(resp http.ResponseWriter, req *http.Request) {
	var (
		m   = req.Method
		obj = new(ObjFile)
		err error
	)

	obj.Md5 = strings.ToLower(req.FormValue(P_MD5))
	if len(obj.Md5) != 32 {
		resp.Write(tools.Json2Byte(403, "无效md5: "+obj.Md5))
		return
	}
	switch {
	case m == "GET": // 获取文件, 带token过来，通过文件名
		obj.SendFile(resp, req)
	case m == "PUT": // 新建文件
		if err = obj.FileServer(resp, req); err != nil {
			resp.Write(tools.Json2Byte(500, err.Error()))
		} else {
			resp.Write(tools.Json2Byte(200, "成功上传: "+obj.Md5))
		}

	case m == "DELETE": // 删除文件，通过文件名
		obj.DeleteFile(resp, req)
	default: // 非法Method返回405
		resp.Write(tools.Json2Byte(405, Err405.Error()))
	}
}

// 通过md5返回信息
func handlerCheckFile(resp http.ResponseWriter, req *http.Request) {
	var obj = new(ObjFile)

	obj.Md5 = strings.ToLower(req.FormValue(P_MD5))
	if len(obj.Md5) != 32 {
		resp.Write(tools.Json2Byte(403, "无效md5: "+obj.Md5))
		return
	}
	if req.Method != "GET" {
		resp.Write(tools.Json2Byte(405, "非法Method"))
		return
	}
	// 检查是否存在该文件，并返回元数据信息
	obj.HeadFile(resp, req)
}
