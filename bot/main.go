package main

import (
	"encoding/json"
	"log"
	"os"
	"regexp"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/google/uuid"
)

var (
	shortsRe = regexp.MustCompile(`(?i)^https?://(www\.)?(youtube\.com/shorts/|youtu\.be/)[^\s]+$`)
)

type Task struct {
	ID     string `json:"id"`
	URL    string `json:"url"`
	ChatID int64  `json:"chat_id,omitempty"`
}

type Result struct {
	ID     string `json:"id"`
	URL    string `json:"url"`
	S3URL  string `json:"s3_url"`
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	ChatID int64  `json:"chat_id,omitempty"`
}

func main() {
	botToken := os.Getenv("BOT_TOKEN")
	if botToken == "" {
		log.Fatal("BOT_TOKEN is required")
	}

	publisher, err := NewPublisherFromEnv()
	if err != nil {
		log.Fatalf("publisher init: %v", err)
	}
	defer publisher.Close()

	bot, err := tgbotapi.NewBotAPI(botToken)
	if err != nil {
		log.Fatalf("bot init: %v", err)
	}
	bot.Debug = false

	s3Client, err := NewS3ClientFromEnv()
	if err != nil {
		log.Fatalf("s3 client init: %v", err)
	}

	consumer, err := NewConsumerFromEnv()
	if err != nil {
		log.Fatalf("consumer init: %v", err)
	}
	defer consumer.Close()

	go func() {
		if err := consumer.Consume(bot, s3Client); err != nil {
			log.Fatalf("consumer error: %v", err)
		}
	}()

	u := tgbotapi.NewUpdate(0)
	u.Timeout = 30
	updates := bot.GetUpdatesChan(u)

	log.Println("bot started")
	for update := range updates {
		if update.Message == nil {
			continue
		}
		if !update.Message.IsCommand() || update.Message.Command() != "download" {
			continue
		}
		args := update.Message.CommandArguments()
		if args == "" || !shortsRe.MatchString(args) {
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Укажи корректный YouTube Shorts URL: /download <url>")
			msg.ReplyToMessageID = update.Message.MessageID
			bot.Send(msg)
			continue
		}

		taskID := uuid.New().String()
		task := Task{ID: taskID, URL: args, ChatID: update.Message.Chat.ID}
		body, _ := json.Marshal(task)

		if err := publisher.Publish(body); err != nil {
			log.Printf("publish error: %v", err)
			msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Ошибка публикации задачи, попробуй позже")
			msg.ReplyToMessageID = update.Message.MessageID
			bot.Send(msg)
			continue
		}
		msg := tgbotapi.NewMessage(update.Message.Chat.ID, "Задача принята, id="+taskID)
		msg.ReplyToMessageID = update.Message.MessageID
		bot.Send(msg)
		// small sleep to avoid flooding in tests
		time.Sleep(50 * time.Millisecond)
	}
}
