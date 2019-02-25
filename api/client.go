package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"time"

	tools "../tools"
)

// 实现上传/下载切片
// ①下载切片
//    md5: 文件的md5
//
// ②上传切片
//    md5: 文件的md5
//    uploadfile: 文件内容
//    path: 文件在data中的存储位置(后续可以去掉该选项)
//
// ③删除切片
//    md5/path

var (
	CheckGoroutine = time.Second               // 检查频率
	FailCount      = 5                         // 容忍fail次数
	UploadTimeOut  = time.Second * 10          // 上传超时
	URL_PUT        = "http://%s/shard"         // 上传文件
	URL_DELETE     = "http://%s/shard"         // 删除切片
	URL_GET        = "http://%s/shard?%s"      // 下载文件
	URL_CHECK      = "http://%s/checkshard?%s" // 检查切片
	LastServer     string                      // 上一次返回的data服，用于随机返回
)
var (
	ErrNoDataServer   = errors.New("无DataServer服务器")
	ErrServer500      = errors.New("Server 500 Error")
	ErrGetShard       = errors.New("获取切片出错")
	ErrShardNotEnough = errors.New("切片数不足")
)

// 只存放切片的少部分数据
type ObjShard struct {
	Md5      string `json:"md5"`
	BaseName string `json:"base_name"` // 存放在data中的路径，如shards.xx，一般为md5.01这类名
	Server   string `json:"server"`    // 存放在哪个区服, 例如: 0.0.0.0:8000
}

type Sha []ObjShard

// 删除
func (s *Sha) DeleteShard() error {
	var (
		shard   *ObjShard
		length  = len(*s)
		succ    int
		succArr = make([]int, length)
		err     error
	)
	for i := 0; i < FailCount; i++ {
		succ = length
		for j := 0; j < length; j++ {
			shard = &(*s)[j]
			if succArr[j] == 1 {
				continue
			}
			if err = deleteOne(shard); err != nil {
				log.Println(err.Error())
				succ--
			} else {
				succArr[j] = 1
			}
		}
		if succ == length {
			break
		}
	}

	if succ != length {
		return ErrServer500
	}
	// 并没有ES中file类型的文档,等core代码中执行
	return nil
}

// 只要不存在数据，即返回nil，相当于删除成功
func deleteOne(s *ObjShard) error {
	var (
		res  = &tools.Res{}
		body []byte
		buf  = &bytes.Buffer{}
		w    = multipart.NewWriter(buf)
		req  *http.Request
		resp *http.Response
		err  error
	)
	w.WriteField(P_MD5, s.Md5)
	w.WriteField(P_PATH, s.BaseName)
	boundary := w.Boundary()
	if err = w.Close(); err != nil {
		log.Println("制作form出错: ", err.Error())
		return err
	}
	// 生成req请求之前，先关闭表单,留body
	if req, err = http.NewRequest("DELETE", fmt.Sprintf(URL_DELETE, s.Server), buf); err != nil {
		log.Println("构造request出错: ", err.Error())
		return err
	}
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	if resp, err = http.DefaultClient.Do(req); err != nil {
		return err
	}
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	json.Unmarshal(body, res)
	if res.Code == 500 {
		log.Println("切片删除失败: ", s.Server, s.BaseName)
		return errors.New(res.Msg)
	}
	log.Printf("成功删除切片: [%s] %s\n", s.Server, s.BaseName)
	tmp := filepath.Join(TmpDir, s.BaseName)
	log.Println("临时文件: ", tmp)
	if tools.FileExist(tmp) {
		log.Println("删除临时文件: ", tmp)
		os.Remove(tmp) // 删除api中的临时文件
	}
	return nil
}

// 下载
func (s *Sha) DownloadShard() error {
	var (
		shard     ObjShard
		length    = len(*s)
		shardPath string // 切片存放位置
		succ      int    // 成功次数
		err       error
	)
	for i := 0; i < FailCount; i++ {
		succ = length
		for _, shard = range *s {
			shardPath = filepath.Join(TmpDir, shard.BaseName)
			if tools.FileExist(shardPath) && isNotZero(shardPath) {
				log.Println("存在该切片: ", shardPath)
				continue
			}
			if err = downOne(&shard); err != nil {
				log.Println("下载切片出错: ", shard.BaseName)
				succ--
			}
		}
		// 提前退出
		if succ == length {
			break
		}
		time.Sleep(CheckGoroutine)
	}
	if succ < DATA_C {
		log.Println("有效切片过少: ", succ)
		return ErrShardNotEnough
	}
	return nil
}

func downOne(s *ObjShard) error {
	var (
		dest = filepath.Join(TmpDir, s.BaseName)
		f, _ = os.Create(dest)
		resp *http.Response
		v    = url.Values{}
		body []byte
		res  = &tools.Res{}
		err  error
	)
	v.Add(P_MD5, s.Md5)
	if resp, err = http.Get(fmt.Sprintf(URL_CHECK, s.Server, v.Encode())); err != nil {
		log.Println(err)
		return ErrGetShard
	}
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	json.Unmarshal(body, res)
	if res.Code != 302 {
		log.Println(res.Msg, s.BaseName)
		return ErrGetShard
	}
	// 添加token，获取文件
	v.Add(P_TOKEN, res.Msg)
	if resp, err = http.Get(fmt.Sprintf(URL_GET, s.Server, v.Encode())); err != nil {
		log.Println(err)
		return ErrServer500
	}
	// 复制响应到指定临时文件
	body, _ = ioutil.ReadAll(resp.Body)
	f.Write(body)
	log.Println("切片获取成功: ", dest)
	return nil
}

// 判断文件是否非0
func isNotZero(filePath string) bool {
	f, _ := os.Open(filePath)
	finfo, _ := f.Stat()
	return finfo.Size() != 0
}

// 该包主要用于向DataServer提交数据
func (s *Sha) UploadShard(obj *ObjFile, shardArr []string) {
	var (
		length = len(shardArr)
		succ   int // 成功次数
		//shard  = make([]ObjShard, DATA_C+PARITY_C)
	)
	for i := 0; i < FailCount; i++ {
		succ = length
		for j := 0; j < length; j++ {
			if shardArr[j] == "" { // 跳过已经置""的string
				continue
			}
			//shard[j] = ObjShard{}
			(*s)[j] = ObjShard{}
			// uploadOne(&shardArr[j], &shard[j])
			uploadOne(&shardArr[j], &(*s)[j])
			if shardArr[j] != "" {
				succ--
				log.Println("分片上传失败: ", shardArr[j])
			}
		}
		// 提前跳出循环体
		if succ == length {
			break
		}
		time.Sleep(CheckGoroutine)
	}
	// obj.ObjShard = shard
	obj.ObjShard = *s
	if _, err := ESearch.Add(ES_TYPE_FILE, obj.Md5, obj); err != nil {
		log.Println("提交到ES时出错: ", err.Error())
	}
	log.Println("提交至ES: ", obj.Md5)
}

// 上传一个分片
func uploadOne(src *string, s *ObjShard) {
	var (
		body     = &bytes.Buffer{}
		w        = multipart.NewWriter(body)
		server   string // Data Server地址
		md5      string
		boundary string // 需要设置http头部
		f        *os.File
		path     = filepath.Base(*src)
		req      *http.Request  // 请求体
		resp     *http.Response // 响应体
		res      = &tools.Res{}
		part     io.Writer
		err      error
	)
	if server, err = getOneServer(); err != nil {
		log.Println(err.Error())
		return
	}
	if md5, err = tools.Md5Get(*src); err != nil {
		log.Println("获取MD5失败: ", err.Error())
		return
	}
	// 制作表单
	f, _ = os.Open(*src)
	if part, err = w.CreateFormFile(P_FILE, path); err != nil {
		log.Println(err.Error())
		return
	}
	io.Copy(part, f)
	// 其他额外数据
	w.WriteField(P_MD5, md5)
	w.WriteField(P_PATH, path)
	boundary = w.Boundary()
	if err = w.Close(); err != nil {
		log.Println("制作form表单时出错: ", err.Error())
		return
	}

	// 上传数据
	if req, err = http.NewRequest("PUT", fmt.Sprintf(URL_PUT, server), body); err != nil {
		log.Println("构造request时出错: ", err.Error())
		return
	}
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	cli := http.DefaultClient
	cli.Timeout = UploadTimeOut // 上传超时
	if resp, err = cli.Do(req); err != nil {
		log.Println(err.Error())
		return
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(res); err != nil {
		log.Println("切片上传失败: ", *src, err.Error())
		return
	}
	if res.Code != 200 {
		log.Println("执行PUT失败: ", res.Msg)
		return
	}
	s.BaseName = path
	s.Md5 = md5
	s.Server = server
	log.Printf("success: [%s] <- %s\n", server, *src)
	// 清除切片信息
	os.Remove(*src)
	*src = "" // 将slice中相对应字段置nil
	return
}

// 从DataServer中获取一个dataserver，如果len为0，则返回nil
func getOneServer() (string, error) {
	var (
		l int = len(DataServer)
		i int
	)
	if l == 0 {
		return "", ErrNoDataServer
	}
	// 是否需要加读锁
	for server, _ := range DataServer {
		i++
		if i != l && LastServer == server {
			continue
		}
		LastServer = server
		return server, nil
	}
	return "", nil
}
