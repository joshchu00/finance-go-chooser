package twse

import (
	"fmt"
	"strconv"

	"github.com/golang/protobuf/proto"
	"github.com/joshchu00/finance-go-common/cassandra"
	"github.com/joshchu00/finance-go-common/datetime"
	"github.com/joshchu00/finance-go-common/kafka"
	"github.com/joshchu00/finance-go-common/logger"
	"github.com/joshchu00/finance-go-common/strategy"
	protobuf "github.com/joshchu00/finance-protobuf/inside"
)

func Process(symbol string, period string, ts int64, client *cassandra.Client, producer *kafka.Producer, topic string) (err error) {

	logger.Info(fmt.Sprintf("%s: %d %s", "Starting twse.Process...", ts, symbol))

	var irs []*cassandra.IndicatorRow

	irs, err = client.SelectIndicatorRowsByPartitionKey(
		&cassandra.IndicatorPartitionKey{
			Exchange: "TWSE",
			Symbol:   symbol,
			Period:   period,
		},
	)
	if err != nil {
		return
	}

	in := make([]*strategy.LSMAInput, 0)

	for _, ir := range irs {

		var sma0060, sma0120, sma0240 float64
		sma0060, err = strconv.ParseFloat(ir.SMA0060.String(), 64)
		if err != nil {
			return
		}
		sma0120, err = strconv.ParseFloat(ir.SMA0120.String(), 64)
		if err != nil {
			return
		}
		sma0240, err = strconv.ParseFloat(ir.SMA0240.String(), 64)
		if err != nil {
			return
		}

		in = append(
			in,
			&strategy.LSMAInput{
				SMA0060: sma0060,
				SMA0120: sma0120,
				SMA0240: sma0240,
			},
		)
	}

	stg := strategy.LSMA

	values := strategy.CalculateLSMA(in)

	for i, ir := range irs {

		if datetime.GetTimestamp(ir.Datetime) >= ts {

			value := values[i]

			client.InsertStrategyRowStringColumn(
				&cassandra.StrategyPrimaryKey{
					StrategyPartitionKey: cassandra.StrategyPartitionKey{
						Exchange: "TWSE",
						Symbol:   symbol,
						Period:   period,
					},
					Datetime: ir.Datetime,
				},
				stg.Column,
				string(value),
			)
		}
	}

	lastI := len(irs) - 1
	lastIR := irs[lastI]
	lastValue := values[lastI]

	if lastValue != strategy.LSMANIL {

		message := &protobuf.Notifier{
			Exchange: "TWSE",
			Symbol:   symbol,
			Period:   period,
			Datetime: datetime.GetTimestamp(lastIR.Datetime),
			Strategy: "lsma",
		}

		var bytes []byte

		bytes, err = proto.Marshal(message)
		if err != nil {
			return
		}

		err = producer.Produce(topic, 0, bytes)
		if err != nil {
			return
		}
	}

	return
}
