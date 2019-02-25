package tools

import (
	"fmt"
	"log"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

// 心跳相关信息
type HeartBeat struct {
	NSQAddr      string        // nsq地址
	ProdTopic    string        // 需要发送至哪个topic
	ProdMsg      string        // 发送的消息
	WarnCount    int           // 触发警报次数
	ProdInterval time.Duration // 发送的时间间隔

	Config    *nsq.Config
	Producer  *nsq.Producer
	VaildTime int64 // 有效时间
}

// 初始化心跳信息
// 生产者: nsq为nsq地址，prodMsg一般为需要发送的消息, interval为发送消息的时间间隔
func NewHeartBeat(nsqAddr, prodTopic, prodMsg string, warnCount int, prodInterval time.Duration) *HeartBeat {
	var (
		conf = nsq.NewConfig()
		err  error
		prod *nsq.Producer
	)
	if prod, err = nsq.NewProducer(nsqAddr, conf); err != nil {
		log.Fatalln(err)
	}
	return &HeartBeat{
		NSQAddr:      nsqAddr,
		ProdTopic:    prodTopic,
		ProdMsg:      prodMsg,
		WarnCount:    warnCount,
		ProdInterval: prodInterval,
		Config:       conf,
		Producer:     prod,
	}
}

// 发送心跳信息
func (h *HeartBeat) SendHeart() {
	var (
		msg  string // 要发布的信息
		fail int
		err  error
	)
	for {
		msg = fmt.Sprintf("%d,%s", time.Now().UnixNano(), h.ProdMsg)
		//	log.Println("发布HB信息:", h.ProdTopic, " <- ", msg)
		if err = h.Producer.Publish(h.ProdTopic, []byte(msg)); err != nil {
			fail++
			if fail >= h.WarnCount {
				log.Println("HB发布失败: ", fail)
			}
			time.Sleep(h.ProdInterval / 2)
			continue
		}
		time.Sleep(h.ProdInterval)
		fail = 0
	}
}

// 发送任意主题的信息
func (h *HeartBeat) Send(topic, msg string) error {
	return h.Producer.Publish(topic, []byte(msg))
}

// 已经实现的handle,api和data中的handle可能不一致
func (h *HeartBeat) AddConsumer(topic, channel string, handler nsq.Handler) {
	var (
		consumer *nsq.Consumer
		err      error
	)
	if consumer, err = nsq.NewConsumer(topic, channel, h.Config); err != nil {
		log.Fatalln(err)
	}
	consumer.AddHandler(handler)
	if err = consumer.ConnectToNSQD(h.NSQAddr); err != nil {
		log.Fatalln(err)
	}
}
