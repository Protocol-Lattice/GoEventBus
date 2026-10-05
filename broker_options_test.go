package GoEventBus

import (
	"errors"
	"testing"
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
