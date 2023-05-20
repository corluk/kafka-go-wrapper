package kafka_wrapper

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"

	"github.com/segmentio/kafka-go"
)

type KafkaWrapper struct {
	WriterConfig     kafka.WriterConfig
	ReaderConfig     kafka.ReaderConfig
	KafkaTopicConfig kafka.TopicConfig
}

type any = interface{}

func (kafkaWrapper *KafkaWrapper) prepareMessage(value any) (kafka.Message, error) {
	var message kafka.Message
	encodedMessage, err := kafkaWrapper.encodeMessage(value)
	if err != nil {
		return message, err
	}
	message.Value = encodedMessage
	return message, nil
}
func (kafkaWrapper *KafkaWrapper) encodeMessage(value any) ([]byte, error) {

	return json.Marshal(value)
}

func (kafkaWrapper *KafkaWrapper) NewWriter() *kafka.Writer {

	writer := kafka.NewWriter(kafkaWrapper.WriterConfig)

	return writer

}

func (kafkaWrapper *KafkaWrapper) NewReader() *kafka.Reader {

	reader := kafka.NewReader(kafkaWrapper.ReaderConfig)

	return reader

}

func (kafkaWrapper *KafkaWrapper) Send(msgs ...kafka.Message) error {

	writer := kafkaWrapper.NewWriter()
	err := writer.WriteMessages(context.Background(), msgs...)
	if err != nil {
		return err
	}
	return writer.Close()
}
func (kafkaWrapper *KafkaWrapper) SendValue(value any) error {
	message, err := kafkaWrapper.prepareMessage(value)
	if err != nil {
		return err
	}
	return kafkaWrapper.Send(message)
}

func (kafkaWrapper *KafkaWrapper) SendBatch(values []any) error {
	var messages []kafka.Message
	for _, value := range values {

		message, err := kafkaWrapper.prepareMessage(value)
		if err != nil {
			return err
		}
		messages = append(messages, message)

	}
	return kafkaWrapper.Send(messages...)
}

func (kafkaWrapper *KafkaWrapper) CreateTopic(conn *kafka.Conn) error {

	controller, err := conn.Controller()
	if err != nil {
		return err
	}
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()
	err = controllerConn.CreateTopics(kafkaWrapper.KafkaTopicConfig)
	if err != nil {
		return err
	}
	return nil
}
func (kafkaWrapper *KafkaWrapper) Read(onRead func(value kafka.Message) error) {

	reader := kafkaWrapper.NewReader()
	defer reader.Close()
	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Printf("error %s", err.Error())
		}

		err = onRead(message)
		if err != nil {
			fmt.Printf("error %s", err.Error())

		}
	}

}
func i() {
	topic := "my-topic"

	conn, err := kafka.Dial("tcp", "localhost:9092")
	if err != nil {
		panic(err.Error())
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		panic(err.Error())
	}
	var controllerConn *kafka.Conn
	controllerConn, err = kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		panic(err.Error())
	}
	defer controllerConn.Close()

	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		panic(err.Error())
	}
}
