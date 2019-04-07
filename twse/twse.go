package twse

import (
	"fmt"
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	protobuf "github.com/joshchu00/finance-protobuf/inside"
)

var location *time.Location

func Init() {
	var err error
	location, err = time.LoadLocation("Asia/Taipei")
	if err != nil {
		log.Fatalln("FATAL", "Get location error:", err)
	}
}

func Process(symbol string, period string, ts int64, client *cassandra.Client, producer *kafka.Producer, topic string) (err error) {

	logger.Info(fmt.Sprintf("%s: %s", "Starting twse process...", datetime.GetTimeString(ts, location)))

	message := &protobuf.Notifier{
		Exchange: "TWSE",
		Symbol:   symbol,
		Period:   period,
		Datetime: ts,
		Strategy: "lsma",
	}

	var bytes []byte

	bytes, err = proto.Marshal(message)
	if err != nil {
		return
	}

	producer.Produce(topic, 0, bytes)

	return
}
