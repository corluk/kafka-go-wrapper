package kafka_wrapper

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type KafkaWrapper struct {
	WriterConfig kafka.WriterConfig
	ReaderConfig kafka.ReaderConfig
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

func (kafkaWrapper *KafkaWrapper) SendSingleValue(value any) {

	writer := kafkaWrapper.NewWriter()
	message =
		writer.WriteMessages(context.Background())
}
