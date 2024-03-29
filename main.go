package main

import (
	"errors"
	"fmt"
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
			Name:        "Download DLQ Messages",
			Description: "Downloads messages from Azure Service Bus DLQ.",
			Action: func() error {
				err := WriteDLQMessagesToFile()
				if err != nil {
					return fmt.Errorf("could not write DLQ messages to file: %w", err)
				}

				listCommands()

				return nil
			},
		},
		{
			Name:        "Process Messages",
			Description: "Processes messages in some way.",
			Action: func() error {
				return errors.New("not implemented")
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

func main() {
	GetConnectionString()
	listCommands()
}
