/*
Copyright © 2024 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/laurentpoirierfr/kafka-console/pkg/helpers"
	"github.com/spf13/cobra"
)

// produceCmd represents the produce command
var produceCmd = &cobra.Command{
	Use:   "produce",
	Short: "A brief description of your command",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Run: func(cmd *cobra.Command, args []string) {
		produce()
	},
}

var (
	topic_name      *string
	bootstrap_serve *string

	headers_file *string
	payload_file *string
)

func init() {
	rootCmd.AddCommand(produceCmd)
	topic_name = produceCmd.PersistentFlags().StringP("topic", "t", "topic-name", "Topic name")
	bootstrap_serve = produceCmd.PersistentFlags().StringP("bootstrap-server", "b", "localhost:9092", "Bootstrap Server")
	headers_file = produceCmd.PersistentFlags().StringP("headers-file", "d", "json/headers.json", "Headers json file")
	payload_file = produceCmd.PersistentFlags().StringP("payload_file", "p", "json/payload.json", "Payload json file")
}

func produce() {
	fmt.Println("produce called")

	value, err := os.ReadFile(*payload_file)
	check(err)

	headers, err := os.ReadFile(*headers_file)
	check(err)

	var mapHeaders map[string]string
	err = json.Unmarshal(headers, &mapHeaders)
	check(err)

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": *bootstrap_serve,
	})
	check(err)
	defer p.Close()

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: topic_name, Partition: kafka.PartitionAny},
		Value:          value,
		Headers:        helpers.MapToKafkaHeaders(mapHeaders),
	}, nil)

	check(err)
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
