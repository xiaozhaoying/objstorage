package tools

import (
	"context"
	"fmt"
	"log"

	"gopkg.in/olivere/elastic.v5"
)

// 封装ES，实现CRUD增删改查

type ES struct {
	ElasticURL string
	IndexName  string          // index名
	Client     *elastic.Client // 存放客户端
}

func NewES(addr, index string) *ES {
	var (
		c        *elastic.Client
		err      error
		exists   bool
		indexAck *elastic.IndicesCreateResult
	)
	if c, err = elastic.NewClient(elastic.SetURL(addr)); err != nil {
		log.Fatalln(err)
	}
	if exists, err = c.IndexExists(index).Do(context.Background()); err != nil {
		log.Fatalf("查询index索引时出错: %s\n", err)
	}
	if !exists {
		if indexAck, err = c.CreateIndex(index).Do(context.Background()); err != nil {
			log.Fatalf("创建index索引失败: %s\n", err.Error())
		}
		if !indexAck.Acknowledged {
			log.Fatalln("无法确认是否创建索引")
		}
	} else {
		log.Printf("已存在index索引: %s\n", index)
	}
	return &ES{
		ElasticURL: addr,
		IndexName:  index,
		Client:     c,
	}
}

// 增加: 整个doc增加,md5为id
func (es *ES) Add(docType, md5 string, doc interface{}) (string, error) {
	_, err := es.Client.Index().
		Index(es.IndexName).
		Type(docType).
		Id(md5).
		BodyJson(doc).
		Do(context.Background())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("添加成功: %s, %s", docType, md5), nil
}

// 删除: 根据id/md5
func (es *ES) Delete(docType, md5 string) (string, error) {
	_, err := es.Client.Delete().
		Index(es.IndexName).
		Type(docType).
		Id(md5).
		Do(context.Background())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("删除成功: %s, %s", docType, md5), nil
}

// 改: 修改整个doc
func (es *ES) UpdateDoc(docType, md5 string, doc interface{}) (string, error) {
	_, err := es.Client.Update().
		Index(es.IndexName).
		Type(docType).
		Id(md5).
		Doc(doc).
		Do(context.Background())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("文档修改成功: %s, %s", docType, md5), nil
}

// 改: 修改部分filed
func (es *ES) UpdateField(docType, md5, filed string, value interface{}) (string, error) {
	_, err := es.Client.Update().
		Index(es.IndexName).
		Type(docType).
		Id(md5).
		Doc(map[string]interface{}{filed: value}).
		Do(context.Background())
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("文档字段成功: %s, %s, %s", docType, md5, filed), nil

}

// 查找: 根据id/md5查找
func (es *ES) GetOne(docType, md5 string) (*elastic.GetResult, error) {
	res, err := es.Client.Get().
		Index(es.IndexName).
		Type(docType).
		Id(md5).
		Do(context.Background())
	if err != nil {
		return nil, err
	}
	return res, nil
}

// 判断是否存在
func (es *ES) IsExists(docType, md5 string) bool {
	res, err := es.Client.Exists().
		Index(es.IndexName).
		Type(docType).
		Id(md5).
		Do(context.Background())
	if err != nil {
		return false
	}
	return res
}
