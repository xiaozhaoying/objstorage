package main

import (
	"log"
	"net/http"
	"time"

	tools "../tools"
)

const (
	P_TOKEN = "token" // token的名称
	P_MD5   = "md5"
	P_PATH  = "path"
	P_FILE  = "uploadfile" // post上传时表单名字
)

var (
	Dir          string                  // 全路径
	ListenAddr   = "192.168.10.150:8000" // 该api服务接听的地址
	ReadTimeout  = 10 * time.Second
	WriteTimeout = 10 * time.Second
)

type DataServerStruct struct {
	ListenAddr string
	Dir        string       // 全路径
	serv       *http.Server // 对外隐藏，提供Start方法替代
}

// 启动程序
func (ds *DataServerStruct) Start() {
	log.Printf("启动data服务: %s\n", ds.serv.Addr)
	log.Fatalln(ds.serv.ListenAndServe())
}

// 关闭程序
func (ds *DataServerStruct) Stop() {}

func NewDataServer() *DataServerStruct {
	var s = http.NewServeMux()

	// 初始化处理函数
	s.HandleFunc("/shard", handlerShard)
	s.HandleFunc("/checkshard", handlerCheckShard)
	s.HandleFunc("/", func(resp http.ResponseWriter, rsq *http.Request) {
		resp.Write(tools.Json2Byte(403, Err403.Error()))
	})

	return &DataServerStruct{
		ListenAddr: ListenAddr,
		Dir:        Dir,
		serv: &http.Server{
			Addr:           ListenAddr,
			Handler:        s,
			ReadTimeout:    ReadTimeout,
			WriteTimeout:   WriteTimeout,
			MaxHeaderBytes: 1 << 20,
		}}
}

// 分片相关功能
func handlerShard(resp http.ResponseWriter, req *http.Request) {
	var (
		m   = req.Method
		sha = new(Shard)
	)
	switch {
	case m == "GET": // 获取分片, 带token过来
		sha.SendShard(resp, req)
	case m == "DELETE": // 删除切片
		sha.DeleteShard(resp, req)

	case m == "PUT": // 新建分片
		sha.Server = ListenAddr
		sha.Create = time.Now().UnixNano()
		sha.ShardServer(resp, req)

	default:
		resp.Write(tools.Json2Byte(405, Err405.Error()))
	}
}

// 通过md5对比，返回对比信息
func handlerCheckShard(resp http.ResponseWriter, req *http.Request) {
	var sha = new(Shard)

	if req.Method != "GET" {
		resp.Write(tools.Json2Byte(405, "非法Method"))
		return
	}
	sha.CheckShard(resp, req)
}
