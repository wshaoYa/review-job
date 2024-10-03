package job

import (
	"context"
	"encoding/json"
	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/segmentio/kafka-go"
	"review-job/internal/conf"
)
import "github.com/elastic/go-elasticsearch/v8"

// JobServer 流式工作 server
type JobServer struct {
	kafkaReader *kafka.Reader
	esCli       *ESCli
	log         *log.Helper
}

type ESCli struct {
	*elasticsearch.TypedClient
	index string
}

// NewJobServer JobServer 构造函数
func NewJobServer(kafkaReader *kafka.Reader, esCli *ESCli, logger log.Logger) *JobServer {
	return &JobServer{
		kafkaReader: kafkaReader,
		esCli:       esCli,
		log:         log.NewHelper(logger),
	}
}

// NewKafkaReader KafkaReader 构造函数
func NewKafkaReader(c *conf.Kafka) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: c.Brokers,
		GroupID: c.GroupId,
		Topic:   c.Topic,
	})
}

// NewESCli *elasticsearch.TypedClient构造函数
func NewESCli(c *conf.ES) *ESCli {
	cfg := elasticsearch.Config{
		Addresses: c.Addresses,
	}

	client, err := elasticsearch.NewTypedClient(cfg)
	if err != nil {
		log.Errorf("job review NewESCli elasticsearch.NewClient failed,err:%v\n", err)
		panic(err)
	}

	return &ESCli{
		TypedClient: client,
		index:       c.Index,
	}
}

func (js *JobServer) Start(ctx context.Context) error {
	js.log.Debug("JobServer Start...\n")

	for {
		//读取kafka数据
		m, err := js.kafkaReader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				js.log.Debugf("context Canceled\n")
				return nil
			}
			js.log.Warnf("job js.kafkaReader.ReadMessage failed,err:%v\n", err)
			return err
		}

		//debug
		js.log.Debugf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))

		//转换数据格式
		msg := new(Msg)
		err = json.Unmarshal(m.Value, msg)
		if err != nil {
			js.log.Errorf("job json.Unmarshal(m.Value, msg) failed,err:%v\n", err)
			continue
		}

		// 此处按道理来说应该有业务逻辑 综合几个表的数据处理后再写入ES （此处省略）

		//写入ES
		if msg.Type == "INSERT" {
			for _, d := range msg.Data {
				js.createDocument(d)
			}
		} else if msg.Type == "UPDATE" {
			for _, d := range msg.Data {
				js.updateDocument(d)
			}
		}
	}
}

func (js *JobServer) Stop(ctx context.Context) error {
	js.log.Debug("JobServer Stop...\n")
	return js.kafkaReader.Close()
}

// es中写入doc
func (js *JobServer) createDocument(d map[string]any) {
	reviewID := d["review_id"].(string)
	res, err := js.esCli.Index(js.esCli.index).
		Id(reviewID).
		Document(d).
		Do(context.Background())
	if err != nil {
		js.log.Errorf("job createDocument failed,err:%v\n", err)
		return
	}

	js.log.Debugf("res.Result: %v\n", res.Result)
}

// es中更新doc
func (js *JobServer) updateDocument(d map[string]any) {
	reviewID := d["review_id"].(string)

	//判断es中是否有doc 没有则新建 有则更新
	get, err := js.esCli.Get(js.esCli.index, reviewID).Do(context.Background())
	if err != nil {
		js.log.Errorf("job createDocument js.esCli.Get failed,err:%v\n", err)
		return
	}
	if !get.Found {
		js.createDocument(d)
		return
	}

	res, err := js.esCli.Update(js.esCli.index, reviewID).
		Doc(d).
		Do(context.Background())
	if err != nil {
		js.log.Errorf("job createDocument failed,err:%v\n", err)
		return
	}

	js.log.Debugf("res.Result: %v\n", res.Result)
}
