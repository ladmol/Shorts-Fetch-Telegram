package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type S3Client struct {
	client *minio.Client
}

func NewS3ClientFromEnv() (*S3Client, error) {
	endpointURL := getenv("S3_ENDPOINT_URL", getenv("MINIO_ENDPOINT", "http://minio:9000"))
	accessKey := getenv("AWS_ACCESS_KEY_ID", getenv("MINIO_ROOT_USER", "minioadmin"))
	secretKey := getenv("AWS_SECRET_ACCESS_KEY", getenv("MINIO_ROOT_PASSWORD", "minioadmin"))

	u, err := url.Parse(endpointURL)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}
	useSSL := u.Scheme == "https"
	endpoint := u.Host
	if u.Port() == "" && !useSSL {
		endpoint += ":9000"
	}

	client, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: useSSL,
	})
	if err != nil {
		return nil, fmt.Errorf("minio client: %w", err)
	}
	log.Printf("s3 client initialized: endpoint=%s, bucket=%s", endpoint, getenv("S3_BUCKET", "downloads"))
	return &S3Client{client: client}, nil
}

func (s *S3Client) Download(bucket, key string, w io.Writer) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	obj, err := s.client.GetObject(ctx, bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return fmt.Errorf("get object: %w", err)
	}
	defer obj.Close()
	if _, err := io.Copy(w, obj); err != nil {
		return fmt.Errorf("copy: %w", err)
	}
	return nil
}
