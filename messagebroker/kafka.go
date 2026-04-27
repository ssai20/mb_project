package messagebroker

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/IBM/sarama"
)

type AivenConfig struct {
	Brokers    string
	Username   string
	Password   string
	AccessCert string
	AccessKey  string
	CACert     string
}

func CreativeAivenProducer(config *AivenConfig) (sarama.SyncProducer, error) {
	saramaConfig := sarama.NewConfig()

	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Retry.Max = 5
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Timeout = 10 * time.Second

	saramaConfig.Net.DialTimeout = 30 * time.Second
	saramaConfig.Net.ReadTimeout = 30 * time.Second
	saramaConfig.Net.WriteTimeout = 30 * time.Second

	saramaConfig.Net.TLS.Enable = true

	clientCert, err := tls.X509KeyPair([]byte(config.AccessCert), []byte(config.AccessKey))
	if err != nil {
		return nil, fmt.Errorf("ошибка загрузки клиентского сертификата: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM([]byte(config.CACert)) {
		return nil, fmt.Errorf("ошибка добавления СА сертификата")
	}

	saramaConfig.Net.TLS.Config = &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
	}
	//saramaConfig.Net.SASL.Enable = true
	//saramaConfig.Net.SASL.User = config.Username
	////saramaConfig.Net.SASL.Password = config.Password
	//saramaConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	//saramaConfig.Net.SASL.Handshake = true

	brokers := []string{config.Brokers}
	producer, err := sarama.NewSyncProducer(brokers, saramaConfig)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания продьюсера: %w", err)
	}

	return producer, nil

}

func SendToKafka(producer sarama.SyncProducer, topic string, m map[string]string) error {

	jsonMap, err := json.Marshal(m)
	if err != nil {
		log.Fatalf("Ошибка сериализации: %s", err)
	}
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(jsonMap),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Fatalf("Не удалось отправить сообщение: %s", err)
	}

	log.Printf("Сообщение сохранено в топике %s/ партиции %d/ смещении %d\n", "test_topic", partition, offset)
	return nil
}
