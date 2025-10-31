FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY bot/ ./
# Resolve deps and generate go.sum before building
RUN go mod tidy && go mod download

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/bot ./

FROM alpine:3.20
WORKDIR /app
COPY --from=builder /out/bot /app/bot

ENV RABBITMQ_HOST=rabbitmq \
    RABBITMQ_PORT=5672 \
    RABBITMQ_QUEUE=download.tasks

CMD ["/app/bot"]


