package helpers

import "github.com/confluentinc/confluent-kafka-go/kafka"

func MapToKafkaHeaders(m map[string]string) []kafka.Header {
	headers := make([]kafka.Header, 0, len(m))
	for key, value := range m {
		headers = append(headers, kafka.Header{Key: key, Value: []byte(value)})
	}
	return headers
}

func KafkaHeadersToMap(headers []kafka.Header) map[string]string {
	m := make(map[string]string)
	for _, header := range headers {
		m[header.Key] = string(header.Value)
	}
	return m
}
