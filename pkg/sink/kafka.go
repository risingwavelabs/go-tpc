package sink

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"reflect"
)

type KafkaSink struct {
	topic               string
	partition           int32
	producers           []*kafka.Producer
	keys                []string
	count               int
	maxFlushMsgCount    int
	flushTimeoutSeconds int
}

var _ Sink = &KafkaSink{}

func NewKafkaSink(topic string, producers []*kafka.Producer, partition, flushTimeoutSeconds, maxFlushMsgCount int, keys []string) *KafkaSink {
	return &KafkaSink{
		partition:           int32(partition),
		topic:               topic,
		keys:                keys,
		producers:           producers,
		maxFlushMsgCount:    maxFlushMsgCount,
		flushTimeoutSeconds: flushTimeoutSeconds,
	}
}

func buildJsonRow(keys []string, values []interface{}) ([]byte, error) {
	msg := make(map[string]interface{})
	for i, key := range keys {
		ty := reflect.TypeOf(values[i])
		if ty == nil {
			msg[key] = nil
			continue
		}
		switch ty.Kind() {
		case reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
			reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Float32, reflect.Float64:
			msg[key] = values[i]
			continue
		}
		switch v := values[i].(type) {
		case sql.NullString:
			if v.Valid {
				msg[key] = v.String
			} else {
				msg[key] = nil
			}
		case sql.NullInt64:
			if v.Valid {
				msg[key] = v.Int64
			} else {
				msg[key] = nil
			}
		case sql.NullFloat64:
			if v.Valid {
				msg[key] = v.Float64
			} else {
				msg[key] = nil
			}
		default:
			panic(fmt.Sprintf("unssuported type: %T", v))
		}
	}
	return json.Marshal(msg)
}

func (k *KafkaSink) produce(msg []byte) error {
	k.count++
	if k.count >= k.maxFlushMsgCount {
		k.producers[k.partition].Flush(10 * 1000)
		k.count = 0
	}
	return k.producers[k.partition].Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &k.topic, Partition: k.partition},
		Value:          msg,
	}, nil)
}

func (k *KafkaSink) WriteRow(ctx context.Context, values ...interface{}) error {
	row, err := buildJsonRow(k.keys, values)
	if err != nil {
		return err
	}
	return k.produce(row)
}

func (k *KafkaSink) Flush(ctx context.Context) error {
	k.producers[k.partition].Flush(k.flushTimeoutSeconds * 1000)
	return nil
}

func (k *KafkaSink) Close(ctx context.Context) error {
	k.producers[k.partition].Flush(k.flushTimeoutSeconds * 1000)
	return nil
}
