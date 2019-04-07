package main

import (
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-chooser/twse"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/config"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	protobuf "github.com/joshchu00/finance-protobuf/inside"
)

func init() {

	// config
	config.Init()

	// logger
	logger.Init(config.LogDirectory(), "chooser")

	// log config
	logger.Info(fmt.Sprintf("%s: %s", "EnvironmentName", config.EnvironmentName()))
	logger.Info(fmt.Sprintf("%s: %s", "LogDirectory", config.LogDirectory()))
	logger.Info(fmt.Sprintf("%s: %s", "CassandraHosts", config.CassandraHosts()))
	logger.Info(fmt.Sprintf("%s: %s", "CassandraKeyspace", config.CassandraKeyspace()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaBootstrapServers", config.KafkaBootstrapServers()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaChooserTopic", config.KafkaChooserTopic()))
	logger.Info(fmt.Sprintf("%s: %s", "KafkaNotifierTopic", config.KafkaNotifierTopic()))

	// twse
	twse.Init()
}

var environmentName string

func process() {

	if environmentName == config.EnvironmentNameProd {
		defer func() {
			if err := recover(); err != nil {
				logger.Panic(fmt.Sprintf("recover %v", err))
			}
		}()
	}

	var err error

	// cassandra client
	var cassandraClient *cassandra.Client
	cassandraClient, err = cassandra.NewClient(config.CassandraHosts(), config.CassandraKeyspace())
	if err != nil {
		logger.Panic(fmt.Sprintf("cassandra.NewClient %v", err))
	}
	defer cassandraClient.Close()

	// chooser consumer
	var chooserConsumer *kafka.Consumer
	chooserConsumer, err = kafka.NewConsumer(config.KafkaBootstrapServers(), "chooser", config.KafkaChooserTopic())
	if err != nil {
		logger.Panic(fmt.Sprintf("kafka.NewConsumer %v", err))
	}
	defer chooserConsumer.Close()

	// notifier producer
	var notifierProducer *kafka.Producer
	notifierProducer, err = kafka.NewProducer(config.KafkaBootstrapServers())
	if err != nil {
		logger.Panic(fmt.Sprintf("kafka.NewProducer %v", err))
	}
	defer notifierProducer.Close()

	for {

		message := &protobuf.Chooser{}

		var topic string
		var partition int32
		var offset int64
		var value []byte

		topic, partition, offset, value, err = chooserConsumer.Consume()
		if err != nil {
			logger.Panic(fmt.Sprintf("Consume %v", err))
		}

		err = proto.Unmarshal(value, message)
		if err != nil {
			logger.Panic(fmt.Sprintf("proto.Unmarshal %v", err))
		}

		switch message.Exchange {
		case "TWSE":
			err = twse.Process(message.Symbol, message.Period, message.Datetime, cassandraClient, notifierProducer, config.KafkaNotifierTopic())
			if err != nil {
				logger.Panic(fmt.Sprintf("Process %v", err))
			}
		default:
			logger.Panic("Unknown exchange")
		}

		// strange
		offset++

		err = chooserConsumer.CommitOffset(topic, partition, offset)
		if err != nil {
			logger.Panic(fmt.Sprintf("CommitOffset %v", err))
		}
	}
}

func main() {

	logger.Info("Starting chooser...")

	// environment name
	switch environmentName = config.EnvironmentName(); environmentName {
	case config.EnvironmentNameDev, config.EnvironmentNameTest, config.EnvironmentNameStg, config.EnvironmentNameProd:
	default:
		logger.Panic("Unknown environment name")
	}

	for {

		process()

		time.Sleep(3 * time.Second)

		if environmentName != config.EnvironmentNameProd {
			break
		}
	}
}
