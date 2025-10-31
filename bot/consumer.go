package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Consumer struct {
	conn  *amqp.Connection
	ch    *amqp.Channel
	queue string
}

func NewConsumerFromEnv() (*Consumer, error) {
	host := getenv("RABBITMQ_HOST", "rabbitmq")
	port := getenv("RABBITMQ_PORT", "5672")
	user := getenv("RABBITMQ_USER", getenv("RABBITMQ_DEFAULT_USER", "guest"))
	pass := getenv("RABBITMQ_PASS", getenv("RABBITMQ_DEFAULT_PASS", "guest"))
	queue := getenv("RESULTS_QUEUE", "download.results")

	url := fmt.Sprintf("amqp://%s:%s@%s:%s/", user, pass, host, port)
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, fmt.Errorf("amqp dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("channel: %w", err)
	}
	_, err = ch.QueueDeclare(queue, true, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("queue declare: %w", err)
	}
	err = ch.Qos(1, 0, false)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("qos: %w", err)
	}
	return &Consumer{conn: conn, ch: ch, queue: queue}, nil
}

func (c *Consumer) Close() {
	if c.ch != nil {
		_ = c.ch.Close()
	}
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *Consumer) Consume(bot *tgbotapi.BotAPI, s3Client *S3Client) error {
	msgs, err := c.ch.Consume(c.queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}
	log.Printf("started consuming results queue: %s", c.queue)
	for d := range msgs {
		var result Result
		if err := json.Unmarshal(d.Body, &result); err != nil {
			log.Printf("failed to unmarshal result: %v", err)
			_ = d.Nack(false, false)
			continue
		}
		if err := c.handleResult(bot, s3Client, result); err != nil {
			log.Printf("failed to handle result %s: %v", result.ID, err)
			_ = d.Nack(false, true)
			continue
		}
		_ = d.Ack(false)
	}
	return nil
}

func (c *Consumer) handleResult(bot *tgbotapi.BotAPI, s3Client *S3Client, result Result) error {
	if result.Status != "success" {
		msg := tgbotapi.NewMessage(result.ChatID, fmt.Sprintf("Ошибка скачивания видео: %s", result.Error))
		_, err := bot.Send(msg)
		return err
	}
	log.Printf("processing result: id=%s, s3=%s", result.ID, result.S3URL)
	tmpFile, err := os.CreateTemp("", fmt.Sprintf("download-*.mp4"))
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	defer func() {
		tmpFile.Close()
		os.Remove(tmpFile.Name())
	}()
	if err := s3Client.Download(result.Bucket, result.Key, tmpFile); err != nil {
		return fmt.Errorf("download from s3: %w", err)
	}
	if _, err := tmpFile.Seek(0, 0); err != nil {
		return fmt.Errorf("seek: %w", err)
	}
	fileSize, _ := tmpFile.Seek(0, io.SeekEnd)
	tmpFile.Seek(0, 0)
	maxSize := int64(50 * 1024 * 1024)
	if fileSize > maxSize {
		msg := tgbotapi.NewMessage(result.ChatID, fmt.Sprintf("Файл слишком большой (%d MB), максимум 50 MB", fileSize/(1024*1024)))
		_, _ = bot.Send(msg)
		return nil
	}
	video := tgbotapi.NewVideo(result.ChatID, tgbotapi.FileReader{Name: "video.mp4", Reader: tmpFile})
	video.Caption = fmt.Sprintf("Видео скачано: %s", result.URL)
	if _, err := bot.Send(video); err != nil {
		return fmt.Errorf("send video: %w", err)
	}
	log.Printf("sent video to chat %d for task %s", result.ChatID, result.ID)
	return nil
}
