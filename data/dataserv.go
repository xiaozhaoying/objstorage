package main

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	tools "../tools"
	nsq "github.com/nsqio/go-nsq"
)

const (
	NSQ_ADDR      = "127.0.0.1:4150"
	CONSUMER_TYPE = "DATA" // 消费者类型
)

var (
	DataServerMap  = make(map[string]int64, 8) // 用于保存data节点的时间信息
	DataMu         = &sync.RWMutex{}           // DataServer的读写锁
	HBSendInterval = time.Millisecond * 1000   // 发送心跳时间间隔
	WarnCount      = 5                         // 发送心跳次数超过5次，会打印警报信息
	VaildTime      = time.Second.Nanoseconds() // data超时时间
	Topic          = map[string]string{
		"hbapi":  "HBApiServers",  // api服务器的addr
		"hbdata": "HBDataServers", // data服务器的addr
	}
	// 格式为: 消费者类型_消费者IP_消费者PORT, 例如: API_19216810101_4015
	ChannelAPIHB string            // 获取api心跳信息的channel
	DATASERVER   *DataServerStruct // data的api接口服务器
)

func main() {
	var (
		hb *tools.HeartBeat
	)
	ChannelAPIHB = fmt.Sprintf("%s_%s", CONSUMER_TYPE, strings.Replace(strings.Replace(ListenAddr, ".", "", -1), ":", "_", -1))

	hb = tools.NewHeartBeat(NSQ_ADDR, Topic["hbdata"], ListenAddr, WarnCount, HBSendInterval)
	go hb.SendHeart()
	hb.AddConsumer(Topic["hbapi"], ChannelAPIHB, &APIConsumer{})

	// RESTful
	DATASERVER = NewDataServer()
	DATASERVER.Start()
}

// 获取API节点信息
type APIConsumer struct {
}

func (c *APIConsumer) HandleMessage(msg *nsq.Message) error {
	var (
		m         = msg.Body
		err       error
		t         int64            // unix时间戳
		apiServer = string(m[20:]) // IP地址信息
		now       = time.Now().UnixNano()
	)
	if t, err = strconv.ParseInt(string(m[:19]), 10, 64); err != nil {
		log.Println(err)
		return err
	}
	if now-t > VaildTime {
		log.Printf("API节点超时: [%s] %d\n", apiServer, t)
		return err
	}
	DataMu.Lock()
	DataServerMap[apiServer] = t
	DataMu.Unlock()
	return err
}
