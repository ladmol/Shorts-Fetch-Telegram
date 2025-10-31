package main

import (
    "context"
    "fmt"
    "os"
    "strconv"
    "time"

    amqp "github.com/rabbitmq/amqp091-go"
)

type Publisher struct {
    conn  *amqp.Connection
    ch    *amqp.Channel
    queue string
}

func NewPublisherFromEnv() (*Publisher, error) {
    host := getenv("RABBITMQ_HOST", "rabbitmq")
    port := getenv("RABBITMQ_PORT", "5672")
    user := getenv("RABBITMQ_USER", getenv("RABBITMQ_DEFAULT_USER", "guest"))
    pass := getenv("RABBITMQ_PASS", getenv("RABBITMQ_DEFAULT_PASS", "guest"))
    queue := getenv("RABBITMQ_QUEUE", "download.tasks")
    maxWaitSec := getenvInt("RMQ_CONNECT_TIMEOUT_SECONDS", 60)

    url := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, pass, host, port)
    var conn *amqp.Connection
    var err error
    deadline := time.Now().Add(time.Duration(maxWaitSec) * time.Second)
    backoff := 500 * time.Millisecond
    for {
        conn, err = amqp.Dial(url)
        if err == nil {
            break
        }
        if time.Now().After(deadline) {
            return nil, fmt.Errorf("amqp dial timeout after %ds: %w", maxWaitSec, err)
        }
        time.Sleep(backoff)
        if backoff < 5*time.Second {
            backoff *= 2
        }
    }
    ch, err := conn.Channel()
    if err != nil {
        conn.Close()
        return nil, err
    }
    // Ensure queue exists
    _, err = ch.QueueDeclare(queue, true, false, false, false, nil)
    if err != nil {
        ch.Close()
        conn.Close()
        return nil, err
    }
    return &Publisher{conn: conn, ch: ch, queue: queue}, nil
}

func (p *Publisher) Publish(body []byte) error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()
    return p.ch.PublishWithContext(ctx, "", p.queue, false, false, amqp.Publishing{
        DeliveryMode: amqp.Persistent,
        ContentType:  "application/json",
        Body:         body,
    })
}

func (p *Publisher) Close() {
    if p.ch != nil {
        _ = p.ch.Close()
    }
    if p.conn != nil {
        _ = p.conn.Close()
    }
}

func getenv(k, def string) string {
    if v := os.Getenv(k); v != "" {
        return v
    }
    return def
}

func getenvInt(k string, def int) int {
    if v := os.Getenv(k); v != "" {
        if i, err := strconv.Atoi(v); err == nil {
            return i
        }
    }
    return def
}


