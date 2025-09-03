package main

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/joho/godotenv"
	"log"
	"os"
	"service-bus-hero/prompts"
)

var appContext = &AppContext{}

func listCommands() {
	commands := []prompts.Command{
		{
			Name:        "Topic stats",
			Description: "List stats for all topics.",
			Action: func() error {
				err := ListTopicStatByTopics()
				if err != nil {
					return fmt.Errorf("could not list topic stats: %w", err)
				}

				listCommands()

				return nil
			},
		},
		{
			Name:        "Select Topic",
			Description: "Selects a topic to work with.",
			Action: func() error {
				err := SelectTopic()
				if err != nil {
					return fmt.Errorf("could not select topic: %w", err)
				}

				fmt.Printf("Selected topic: %s\n", appContext.Topic)
				listCommands()

				return nil
			},
		},
		{
			Name:        "Select Subscription",
			Description: "Selects a subscription to work with.",
			Action: func() error {
				err := SelectSubscription()
				if err != nil {
					return fmt.Errorf("could not select subscription: %w", err)
				}

				fmt.Printf("Selected subscription: %s\n", appContext.Subscription)
				listCommands()

				return nil
			},
		},
		{
			Name:        "Download DLQ Messages (PeekLock)",
			Description: "Downloads messages in peek-lock mode",
			Action: func() error {
				err := WriteDLQMessagesToFile(azservicebus.ReceiveModePeekLock)
				if err != nil {
					return fmt.Errorf("could not write DLQ messages to file: %w", err)
				}

				listCommands()

				return nil
			},
		},
		{
			Name:        "Download DLQ Messages (ReceiveAndDelete)",
			Description: "Downloads messages from DLQ and __REMOVES__ them from the queue.",
			Action: func() error {
				err := WriteDLQMessagesToFile(azservicebus.ReceiveModeReceiveAndDelete)
				if err != nil {
					return fmt.Errorf("could not write DLQ messages to file: %w", err)
				}

				listCommands()

				return nil
			},
		},
		{
			Name:        "Publish Messages to topic",
			Description: "Publishes messages to a topic.",
			Action: func() error {
				err := PublishMessages()
				if err != nil {
					return fmt.Errorf("could not publish messages: %w", err)
				}

				listCommands()

				return nil
			},
		},
		{
			Name:        "Change Connection String",
			Description: "Changes the connection string.",
			Action: func() error {
				GetConnectionString()
				return nil
			},
		},
		{
			Name:        "Exit",
			Description: "Exits the application.",
			Action: func() error {
				os.Exit(0)
				return nil
			},
		},
	}

	fmt.Println("")
	PrintContext(appContext)
	prompts.PromptCommandList(commands)
}

func processEnv() {
	// Load the .env file
	err := godotenv.Load() // This will look for a ".env" file in the current directory
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	appContext.ConnectionString = os.Getenv("SBHERO_CONNECTION_STRING")
	appContext.Topic = os.Getenv("SBHERO_TOPIC")
}

func main() {
	processEnv()
	GetConnectionString()
	listCommands()
}
