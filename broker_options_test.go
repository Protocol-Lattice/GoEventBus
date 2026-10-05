package GoEventBus

import (
	"errors"
	"testing"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

func TestWithNATSJetStreamRejectsNilConnectionLazily(t *testing.T) {
	var dispatcher Dispatcher
	store := NewEventStore(&dispatcher, 8, Block, WithNATSJetStream(NATSJetStreamProviderConfig{}))
	err := store.PublishToProvider(nil, Event{Projection: "order.created"})
	if err == nil || err.Error() != "goeventbus: NATS connection must not be nil" {
		t.Fatalf("expected nil NATS connection error, got %v", err)
	}
}

func TestWithKafkaValidatesConfigurationLazily(t *testing.T) {
	var dispatcher Dispatcher
	store := NewEventStore(&dispatcher, 8, Block, WithKafka(KafkaProviderConfig{}))
	err := store.PublishToProvider(nil, Event{Projection: "order.created"})
	if err == nil || err.Error() != "goeventbus: Kafka brokers must not be empty" {
		t.Fatalf("expected Kafka brokers error, got %v", err)
	}
}

func TestKafkaRejectsFixedPartitionWithGroup(t *testing.T) {
	partition := 0
	_, err := NewKafkaProvider(KafkaProviderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "events",
		Group:     "billing",
		Partition: &partition,
	})
	if err == nil {
		t.Fatal("expected group/partition validation error")
	}
}

func TestKafkaGroupUsesZeroValueConfigWithoutPartitionConflict(t *testing.T) {
	provider, err := NewKafkaProvider(KafkaProviderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "events",
		Group:   "billing",
	})
	if err != nil {
		t.Fatalf("expected group config to be valid, got %v", err)
	}
	_ = provider.Close()
}

func TestKafkaRejectsNonStringProjectionBeforeNetwork(t *testing.T) {
	provider, err := NewKafkaProvider(KafkaProviderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "events",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()

	err = provider.Publish(nil, Event{Projection: 123})
	if !errors.Is(err, ErrStringProjection) {
		t.Fatalf("expected ErrStringProjection, got %v", err)
	}
}


func TestRemainingBrokerOptionsValidateLazily(t *testing.T) {
	tests := []struct {
		name string
		opt  EventStoreOption
		want string
	}{
		{"sqs", WithSQS(SQSProviderConfig{}), "goeventbus: SQS client must not be nil"},
		{"sns", WithSNS(SNSProviderConfig{}), "goeventbus: SNS client must not be nil"},
		{"gcp-pubsub", WithGCPPubSub(GCPPubSubProviderConfig{}), "goeventbus: GCP Pub/Sub client must not be nil"},
		{"azure-service-bus", WithAzureServiceBus(AzureServiceBusProviderConfig{}), "goeventbus: Azure Service Bus client must not be nil"},
		{"pulsar", WithPulsar(PulsarProviderConfig{}), "goeventbus: Pulsar client must not be nil"},
		{"mqtt", WithMQTT(MQTTProviderConfig{}), "goeventbus: MQTT client or client options are required"},
		{"postgres", WithPostgres(PostgresProviderConfig{}), "goeventbus: PostgreSQL pool or connection string is required"},
		{"nsq", WithNSQ(NSQProviderConfig{}), "goeventbus: NSQ nsqd address must not be empty"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var dispatcher Dispatcher
			store := NewEventStore(&dispatcher, 8, Block, tt.opt)
			err := store.PublishToProvider(nil, Event{Projection: "order.created"})
			if err == nil || err.Error() != tt.want {
				t.Fatalf("expected %q, got %v", tt.want, err)
			}
		})
	}
}

func TestPostgresRejectsUnsafeTableName(t *testing.T) {
	_, err := NewPostgresProvider(PostgresProviderConfig{
		ConnectionString: "postgres://localhost/test",
		Table:            "events;drop table users",
	})
	if err == nil {
		t.Fatal("expected invalid table name error")
	}
}

func TestMQTTRejectsInvalidQoS(t *testing.T) {
	_, err := NewMQTTProvider(MQTTProviderConfig{
		Client: nil,
		Options: new(mqtt.ClientOptions),
		Topic: "events",
		QoS: 3,
	})
	if err == nil {
		t.Fatal("expected invalid MQTT QoS error")
	}
}
