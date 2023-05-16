package util

import (
	"context"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"time"
)

func CreateTopics(ctx context.Context, kafkaAddr string, tables []string, partitions int) error {
	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": kafkaAddr,
	})
	if err != nil {
		return err
	}
	defer client.Close()
	topics := make([]kafka.TopicSpecification, 0)
	for _, table := range tables {
		topics = append(topics, kafka.TopicSpecification{
			Topic:             table,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		})
	}
	results, err := client.CreateTopics(ctx, topics, kafka.SetAdminOperationTimeout(10*time.Second))
	if err != nil {
		return err
	}
	for _, result := range results {
		StdErrLogger.Printf("create topic result: %s", result.String())
	}
	return nil
}

func DeleteTopics(ctx context.Context, kafkaAddr string, tables []string) error {
	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": kafkaAddr,
	})
	if err != nil {
		return err
	}
	defer client.Close()
	results, err := client.DeleteTopics(ctx, tables, kafka.SetAdminOperationTimeout(10*time.Second))
	if err != nil {
		return err
	}
	for _, result := range results {
		StdErrLogger.Printf("delete topic result: %s", result.String())
	}
	return nil
}
