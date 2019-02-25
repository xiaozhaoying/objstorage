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
	CONSUMER_TYPE = "API" // 消费者类型
)

var (
	DataServer           = make(map[string]int64, 8) // 用于保存data节点的时间信息
	DataMu               = &sync.RWMutex{}           // DataServer的读写锁
	HBSendInterval       = time.Millisecond * 1000   // 发送心跳时间间隔
	WarnCount            = 5                         // 发送心跳次数超过5次，会打印警报信息
	VaildTime      int64 = time.Second.Nanoseconds() // data超时时间，默认：1s
	DealTimeOut          = time.Second * 5           // 3秒钟清理一次过期的DataServer
	Topic                = map[string]string{
		"hbapi":  "HBApiServers",  // api服务器的addr
		"hbdata": "HBDataServers", // data服务器的addr
	}
	// 格式为: 消费者类型_IP_PORT, 其中: 消费者类型为Api|Data
	// 例如: API_19216810101_4015
	ChannelDataHB string           // channel，用于获取Data心跳信息
	APISERVER     *APIServerStruct // api的api接口服务器
)

func main() {
	var (
		hb       *tools.HeartBeat
		datacons = &DataConsumer{ // 消费者
			vaildTime:   VaildTime,
			dealTimeOut: DealTimeOut}
	)
	ChannelDataHB = fmt.Sprintf("%s_%s", CONSUMER_TYPE, strings.Replace(strings.Replace(ListenAddr, ".", "", -1), ":", "_", -1))
	hb = tools.NewHeartBeat(NSQ_ADDR, Topic["hbapi"], ListenAddr, WarnCount, HBSendInterval)
	go hb.SendHeart()
	hb.AddConsumer(Topic["hbdata"], ChannelDataHB, datacons)
	go datacons.dealDataServer() // 启动清理dataserver进程

	// restful
	APISERVER = NewAPIServer()
	APISERVER.Start()
}

// 消费Data节点信息
type DataConsumer struct {
	vaildTime   int64         // 超时时间
	dealTimeOut time.Duration // 处理过期data服的时间间隔
}

func (c *DataConsumer) HandleMessage(msg *nsq.Message) error {
	var (
		m          = msg.Body
		err        error
		t          int64            // unix时间戳
		dataServer = string(m[20:]) // IP地址信息
		now        = time.Now().UnixNano()
	)
	if t, err = strconv.ParseInt(string(m[:19]), 10, 64); err != nil {
		log.Println(err)
		return err
	}
	if now-t > c.vaildTime {
		log.Printf("Data节点超时(剔除该节点): [%s] %d\n", dataServer, t)
		DataMu.Lock()
		delete(DataServer, dataServer)
		DataMu.Unlock()
		return err
	}
	DataMu.Lock()
	if _, ok := DataServer[dataServer]; !ok {
		log.Printf("添加Data节点: [%s]\n", dataServer)
	}
	DataServer[dataServer] = t
	DataMu.Unlock()
	return nil
}

// 清理过期的data服务
func (c *DataConsumer) dealDataServer() {
	var (
		now        int64 // 当前时间
		t          int64 // map中时间
		dataServer string
	)
	for {
		now = time.Now().UnixNano()
		DataMu.Lock()
		for dataServer, t = range DataServer {
			if now-t > c.vaildTime {
				log.Printf("Data节点超时(DataServer): [%s] %d\n", dataServer, t)
				delete(DataServer, dataServer)
			}
		}
		DataMu.Unlock()
		time.Sleep(c.dealTimeOut)
	}
}
