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

	strategies := []*strategy.Strategy{
		strategy.SSMA,
		strategy.LSMA,
	}

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

	ssmaIn := make([]*strategy.SSMAInput, 0)
	lsmaIn := make([]*strategy.LSMAInput, 0)

	for _, ir := range irs {

		var sma0005, sma0010, sma0020, sma0060, sma0120, sma0240 float64

		sma0005, err = strconv.ParseFloat(ir.SMA0005.String(), 64)
		if err != nil {
			return
		}
		sma0010, err = strconv.ParseFloat(ir.SMA0010.String(), 64)
		if err != nil {
			return
		}
		sma0020, err = strconv.ParseFloat(ir.SMA0020.String(), 64)
		if err != nil {
			return
		}

		ssmaIn = append(
			ssmaIn,
			&strategy.SSMAInput{
				SMA0005: sma0005,
				SMA0010: sma0010,
				SMA0020: sma0020,
			},
		)

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

		lsmaIn = append(
			lsmaIn,
			&strategy.LSMAInput{
				SMA0060: sma0060,
				SMA0120: sma0120,
				SMA0240: sma0240,
			},
		)
	}

	var stg *strategy.Strategy

	stg = strategies[0]
	ssmaOut := strategy.CalculateSSMA(ssmaIn)

	for i, ir := range irs {

		if datetime.GetTimestamp(ir.Datetime) >= ts {

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
				string(ssmaOut[i]),
			)
		}
	}

	stg = strategies[1]
	lsmaOut := strategy.CalculateLSMA(lsmaIn)

	for i, ir := range irs {

		if datetime.GetTimestamp(ir.Datetime) >= ts {

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
				string(lsmaOut[i]),
			)
		}
	}

	lastI := len(irs) - 1
	lastIR := irs[lastI]
	lastSSMAOut := ssmaOut[lastI]
	lastLSMAOut := lsmaOut[lastI]

	if lastSSMAOut != strategy.SSMANIL {

		message := &protobuf.Notifier{
			Exchange: "TWSE",
			Symbol:   symbol,
			Period:   period,
			Datetime: datetime.GetTimestamp(lastIR.Datetime),
			Strategy: "ssma",
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

	if lastLSMAOut != strategy.LSMANIL {

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
