package GoEventBus

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var postgresIdentifierRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

type PostgresProviderConfig struct {
	// Pool can be injected and remains application-owned. If Pool is nil,
	// ConnectionString is used to create a pool owned by the provider.
	Pool             *pgxpool.Pool
	ConnectionString string
	Table            string

	PollInterval time.Duration
	RetryDelay   time.Duration
	Codec        EventCodec
}

type PostgresProvider struct {
	pool  *pgxpool.Pool
	owned bool
	table string
	codec EventCodec

	pollInterval time.Duration
	retryDelay   time.Duration

	schemaOnce sync.Once
	schemaErr  error
	closed     atomic.Bool
}

func WithPostgres(config PostgresProviderConfig) EventStoreOption {
	return withProviderFactory(func() (Provider, error) { return NewPostgresProvider(config) })
}

func NewPostgresProvider(config PostgresProviderConfig) (*PostgresProvider, error) {
	if config.Pool == nil && config.ConnectionString == "" {
		return nil, errors.New("goeventbus: PostgreSQL pool or connection string is required")
	}
	if config.Table == "" {
		config.Table = "goeventbus_events"
	}
	if !postgresIdentifierRE.MatchString(config.Table) {
		return nil, fmt.Errorf("goeventbus: invalid PostgreSQL table name %q", config.Table)
	}
	if config.PollInterval <= 0 {
		config.PollInterval = 250 * time.Millisecond
	}
	if config.RetryDelay <= 0 {
		config.RetryDelay = time.Second
	}
	if config.Codec == nil {
		config.Codec = JSONCodec{}
	}

	pool := config.Pool
	owned := false
	if pool == nil {
		var err error
		pool, err = pgxpool.New(context.Background(), config.ConnectionString)
		if err != nil {
			return nil, fmt.Errorf("goeventbus: create PostgreSQL pool: %w", err)
		}
		owned = true
	}

	return &PostgresProvider{
		pool:         pool,
		owned:        owned,
		table:        `"` + config.Table + `"`,
		codec:        config.Codec,
		pollInterval: config.PollInterval,
		retryDelay:   config.RetryDelay,
	}, nil
}

func (p *PostgresProvider) Publish(ctx context.Context, e Event) error {
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	projection, err := remoteProjection(e)
	if err != nil {
		return err
	}
	if err := p.ensureSchema(ctx); err != nil {
		return err
	}
	payload, err := p.codec.Encode(e)
	if err != nil {
		return err
	}
	query := fmt.Sprintf(`INSERT INTO %s (event_id, projection, payload) VALUES ($1, $2, $3)`, p.table)
	if _, err := p.pool.Exec(ctx, query, e.ID, projection, payload); err != nil {
		return fmt.Errorf("goeventbus: publish PostgreSQL event: %w", err)
	}
	return nil
}

func (p *PostgresProvider) Consume(ctx context.Context, consumer EventConsumer) error {
	if consumer == nil {
		return ErrNilConsumer
	}
	if p.closed.Load() {
		return ErrProviderClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := p.ensureSchema(ctx); err != nil {
		return err
	}

	for {
		if p.closed.Load() {
			return ErrProviderClosed
		}
		handled, err := p.consumeOne(ctx, consumer)
		if err != nil {
			return err
		}
		if handled {
			continue
		}
		timer := time.NewTimer(p.pollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func (p *PostgresProvider) consumeOne(ctx context.Context, consumer EventConsumer) (bool, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("goeventbus: begin PostgreSQL consume transaction: %w", err)
	}
	defer tx.Rollback(context.Background())

	var (
		id      int64
		payload []byte
	)
	query := fmt.Sprintf(`
		SELECT id, payload
		FROM %s
		WHERE available_at <= NOW()
		ORDER BY id
		FOR UPDATE SKIP LOCKED
		LIMIT 1`, p.table)
	err = tx.QueryRow(ctx, query).Scan(&id, &payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("goeventbus: select PostgreSQL event: %w", err)
	}

	event, decodeErr := p.codec.Decode(payload)
	if decodeErr != nil {
		if err := p.deferPostgresEvent(ctx, tx, id); err != nil {
			return true, err
		}
		return true, fmt.Errorf("goeventbus: decode PostgreSQL event: %w", decodeErr)
	}
	if err := consumer(ctx, event); err != nil {
		if deferErr := p.deferPostgresEvent(ctx, tx, id); deferErr != nil {
			return true, errors.Join(err, deferErr)
		}
		return true, err
	}

	deleteQuery := fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, p.table)
	if _, err := tx.Exec(ctx, deleteQuery, id); err != nil {
		return true, fmt.Errorf("goeventbus: delete PostgreSQL event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return true, fmt.Errorf("goeventbus: commit PostgreSQL consume transaction: %w", err)
	}
	return true, nil
}

func (p *PostgresProvider) deferPostgresEvent(ctx context.Context, tx pgx.Tx, id int64) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET attempts = attempts + 1,
		    available_at = NOW() + ($2 * INTERVAL '1 millisecond')
		WHERE id = $1`, p.table)
	delayMS := p.retryDelay.Milliseconds()
	if _, err := tx.Exec(ctx, query, id, delayMS); err != nil {
		return fmt.Errorf("goeventbus: defer failed PostgreSQL event: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("goeventbus: commit PostgreSQL retry state: %w", err)
	}
	return nil
}

func (p *PostgresProvider) ensureSchema(ctx context.Context) error {
	p.schemaOnce.Do(func() {
		query := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id BIGSERIAL PRIMARY KEY,
				event_id TEXT NOT NULL DEFAULT '',
				projection TEXT NOT NULL,
				payload BYTEA NOT NULL,
				attempts INTEGER NOT NULL DEFAULT 0,
				available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
				created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
			);
			CREATE INDEX IF NOT EXISTS %s_available_idx
			ON %s (available_at, id)`,
			p.table,
			p.table[1:len(p.table)-1],
			p.table,
		)
		_, p.schemaErr = p.pool.Exec(ctx, query)
		if p.schemaErr != nil {
			p.schemaErr = fmt.Errorf("goeventbus: initialize PostgreSQL event table: %w", p.schemaErr)
		}
	})
	return p.schemaErr
}

func (p *PostgresProvider) Close() error {
	if p.closed.Swap(true) {
		return nil
	}
	if p.owned {
		p.pool.Close()
	}
	return nil
}

var _ Provider = (*PostgresProvider)(nil)
