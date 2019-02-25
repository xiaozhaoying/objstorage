package tools

import (
	"bufio"
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"time"
)

var (
	ErrNotDevice = errors.New("没找到任何网卡")
	ErrNotDir    = errors.New("不为Dir目录")
)

// 获取本地IP地址
func GetLocalIP() (string, error) {
	var (
		ip net.IP
	)
	addrList, err := net.InterfaceAddrs()
	if err != nil {
		return "", ErrNotDevice
	}
	for _, v := range addrList {
		interf, ok := v.(*net.IPNet)
		if !ok {
			continue
		}
		ip = interf.IP
		// 去掉回环地址和ipv6
		if !ip.IsLoopback() && (len(ip.To4()) == net.IPv4len) {
			return ip.String(), nil
		}
	}
	return "", ErrNotDevice
}

// 随机字符串
func RandomString(length int) string {
	var (
		res = make([]byte, length)
		str = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
		l   = len(str)
	)
	rand.Seed(time.Now().UnixNano())
	for m := 0; m < length; m++ {
		res[m] = str[rand.Int()%l]
	}
	return string(res)
}

// 获取文件的md5信息
func Md5Get(src string) (string, error) {
	var (
		f    *os.File
		m    = md5.New()
		bs   = 4096
		pool = make([]byte, bs)
		i    int
		err  error
	)
	if f, err = os.Open(src); err != nil {
		return "", err
	}
	defer f.Close()
	var buf = bufio.NewReaderSize(f, bs)
	for {
		if i, err = buf.Read(pool); err == io.EOF {
			break
		}
		m.Write(pool[:i])
	}
	return fmt.Sprintf("%x", m.Sum(nil)), nil
}

// 根据文件判断MD5是否一致
func MD5Diff(md5Sum, filePath string) bool {
	var (
		f, _ = os.Open(filePath)
		m    = md5.New()
		bs   = 4096
		pool = make([]byte, bs)
		buf  = bufio.NewReaderSize(f, bs)
		err  error
		i    int
	)
	defer f.Close()
	for {
		if i, err = buf.Read(pool); err == io.EOF {
			break
		}
		m.Write(pool[:i])
	}
	return md5Sum == fmt.Sprintf("%x", m.Sum(nil))
}

// 存文件，返回md5校验码
func Storage(r io.Reader, w io.Writer) string {
	var (
		m    = md5.New()
		bs   = 4096
		pool = make([]byte, bs)
		buf  = bufio.NewReaderSize(r, bs)
		i    int
		err  error
	)
	for {
		if i, err = buf.Read(pool); err == io.EOF {
			break
		}
		w.Write(pool[:i])
		m.Write(pool[:i])
	}
	return fmt.Sprintf("%x", m.Sum(nil))
}

// 从中读取文件，边写文件边校验
func MD5AndStorage(r io.Reader, w io.Writer, md5Sum string) bool {
	var (
		m    = md5.New()
		bs   = 4096
		pool = make([]byte, bs)
		buf  = bufio.NewReaderSize(r, bs)
		err  error
		i    int
	)
	for {
		if i, err = buf.Read(pool); err == io.EOF {
			break
		}
		w.Write(pool[:i])
		m.Write(pool[:i])
	}
	return md5Sum == fmt.Sprintf("%x", m.Sum(nil))
}

// 是否存在文件
func FileExist(filePath string) bool {
	if _, err := os.Stat(filePath); err != nil {
		return os.IsExist(err)
	} else {
		return true
	}
}

// 生成dir目录
func DirExist(dirPath string) error {
	if d, err := os.Stat(dirPath); err != nil {
		// 不存在
		return os.MkdirAll(dirPath, 0755)
	} else {
		if d.IsDir() {
			return nil
		} else {
			return ErrNotDir
		}
	}
}

// 接口返回给用户的结果
type Res struct {
	Code uint16 `json:"code"` // 返回http code响应码
	Msg  string `json:"msg"`
}

func Json2Byte(code uint16, msg string) []byte {
	var b = []byte{}
	b, _ = json.Marshal(Res{
		Code: code,
		Msg:  msg,
	})
	return b
}

// 移动文件，不同文件分区时，调用其他方法移动文件
func MoveFile(src, dest string) error {
	var (
		srcD  *os.File
		destD *os.File
		err   error
	)
	if err = os.Rename(src, dest); err == nil {
		return nil
	}
	if destD, err = os.Create(dest); err != nil {
		return err
	}
	srcD, _ = os.Open(src)
	defer destD.Close()
	defer srcD.Close()
	if _, err = io.Copy(destD, srcD); err != nil {
		return err
	}
	os.Remove(src)
	return nil
}
