package consumer

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaMessageConsumer est une implémentation de MessageConsumer utilisant la bibliothèque confluent-kafka-go.
type KafkaMessageConsumer struct {
	kafkaConsumer *kafka.Consumer
	message       *kafka.Message
}

// NewKafkaMessageConsumer crée une nouvelle instance de KafkaMessageConsumer.
func NewKafkaMessageConsumer(kafkaConsumer *kafka.Consumer) *KafkaMessageConsumer {
	return &KafkaMessageConsumer{
		kafkaConsumer: kafkaConsumer,
	}
}

// GetMessage renvoie le message consommé sous forme de []byte.
func (c *KafkaMessageConsumer) GetMessage() []byte {
	if c.message == nil {
		return nil
	}
	return c.message.Value
}

// GetHeaders renvoie les headers associés au message sous forme de map[string]string.
func (c *KafkaMessageConsumer) GetHeaders() map[string]string {
	if c.message == nil {
		return nil
	}
	headers := make(map[string]string)
	for _, header := range c.message.Headers {
		headers[string(header.Key)] = string(header.Value)
	}
	return headers
}

// MessageHandler est une fonction qui prend un KafkaMessageConsumer en entrée et traite le message.
type MessageHandler func(messageConsumer *KafkaMessageConsumer)

// ConsumerInterface définit l'interface pour un consommateur de messages Kafka.
type ConsumerInterface interface {
	Start()
	StartMessageConsumer(handleMessage MessageHandler)
	Close()
}

// Consumer est une classe pour la consommation de messages Kafka.
type Consumer struct {
	consumer *kafka.Consumer
}

// NewConsumer crée une nouvelle instance de Consumer avec le consommateur Kafka donné.
func NewConsumer(consumer *kafka.Consumer) *Consumer {
	return &Consumer{
		consumer: consumer,
	}
}

// Start démarre la consommation de messages Kafka.
func (c *Consumer) Start() {
	c.StartMessageConsumer(func(messageConsumer *KafkaMessageConsumer) {
		fmt.Println("Message consommé:", string(messageConsumer.GetMessage()))
		fmt.Println("Headers:", messageConsumer.GetHeaders())
	})
}

// StartMessageConsumer démarre la consommation de messages Kafka avec une fonction de traitement de messages personnalisée.
func (c *Consumer) StartMessageConsumer(handleMessage MessageHandler) {
	// Création d'un canal pour écouter les signaux système SIGINT et SIGTERM
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Démarrage de la consommation des messages dans une goroutine
	go func() {
		messageConsumer := NewKafkaMessageConsumer(c.consumer)
		for {
			// Consomme un message.
			ev := c.consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				// Utilise handleMessage pour traiter le message.
				messageConsumer.message = e
				handleMessage(messageConsumer)

			case kafka.Error:
				fmt.Fprintf(os.Stderr, "Erreur Kafka: %v\n", e)
				// Arrête la boucle de consommation si une erreur survient.
				if e.Code() == kafka.ErrAllBrokersDown {
					break
				}
			}
		}
	}()

	// Attend un signal système pour terminer le programme proprement
	<-sigchan

	fmt.Println("Signal reçu, arrêt du consommateur Kafka...")
	c.Close()
	fmt.Println("Consommateur Kafka arrêté.")
}

// Close arrête proprement le consommateur Kafka.
func (c *Consumer) Close() {
	c.consumer.Close()
}
