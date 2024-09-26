package main

import (
	"context"
	"encoding/json"
	"github.com/google/go-github/v38/github"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"net/http"
	"os"
	"time"
)

var shutdownCh chan string

func main() {
	log.Println("github-org-webhook-center started")

	goDotErr := godotenv.Load()
	if goDotErr != nil {
		log.Println("Error loading .env file")
	}

	m := http.NewServeMux()
	m.HandleFunc("/onGitHubPush", handleOnGitHubPush)

	var server *http.Server
	shutdownCh = make(chan string)

	addr := ":" + os.Getenv("HTTP_SERVICE_PORT")
	server = &http.Server{Addr: addr, Handler: m}

	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	// 처음 실행할 때 익스체인지를 만들어둔다.
	exchangeName := os.Getenv("RMQ_EXCHANGE_NAME")

	_, err := declareMqExchange(exchangeName)
	if err != nil {
		panic(err)
	}

	select {
	case shutdownMsg := <-shutdownCh:
		_ = server.Shutdown(context.Background())
		log.Printf("Shutdown message: %s", shutdownMsg)
	}

	log.Println("github-org-webhook-center shutdown gracefully")
}

func handleOnGitHubPush(writer http.ResponseWriter, request *http.Request) {
	log.Println("handleOnGitHubPush")

	payload, err := github.ValidatePayload(request, []byte(os.Getenv("GITHUB_SECRET_TOKEN")))
	if err != nil {
		log.Printf("ValidatePayload failed: %v\n", err)
		return
	}

	_, _ = writer.Write([]byte("ok"))

	for {
		if err := publishToMqExchange(payload); err != nil {
			log.Printf("publishToMqExchange error: %v\n", err)
			log.Printf("Retry publishToMqExchange in 2 seconds...")
			<-time.After(2 * time.Second)
		} else {
			break
		}
	}

	if os.Getenv("SHUTDOWN_ON_GITHUB_PUSH") != "1" {
		log.Println("Shutdown request from GitHub push validated, but SHUTDOWN_ON_GITHUB_PUSH is not 1. No shutdown.")
		return
	}

	log.Println(string(payload))
	log.Println("Shutdown by GitHub push.")
	shutdownCh <- "bye"
}

func publishToMqExchange(payload []byte) error {
	exchangeName := os.Getenv("RMQ_EXCHANGE_NAME")

	ch, err := declareMqExchange(exchangeName)
	if err != nil {
		return err
	}

	log.Printf("Payload %v bytes received from GitHub", len(payload))

	// GoLand IDE 콘솔에서 JSON 출력 시 이상하게 바뀌는 부분이 있다. 버그 같으므로 복붙해서 쓸 때 주의
	//log.Println(string(payload))

	var githubPush GitHubPushPayload
	err = json.Unmarshal(payload, &githubPush)
	if err != nil {
		panic(err)
	}

	if len(githubPush.Pusher.Name) == 0 || len(githubPush.Pusher.Email) == 0 || len(githubPush.Repository.FullName) == 0 {
		log.Println("Not a push event")
		return nil
	}

	log.Printf("Pushed on repository %v by %v\n", githubPush.Repository.FullName, githubPush.Pusher.Name)

	routingKey := githubPush.Repository.FullName

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := "push"
	err = ch.PublishWithContext(ctx,
		exchangeName, // exchange
		routingKey,   // routing key
		false,        // mandatory
		false,        // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})

	return nil
}

func declareMqExchange(exchangeName string) (*amqp.Channel, error) {
	conn, err := amqp.Dial(os.Getenv("RMQ_ADDR"))
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.Confirm(false)
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchangeName,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return ch, nil
}
