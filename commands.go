package main

import (
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"log"
	"os"
	"service-bus-hero/io"
	"service-bus-hero/prompts"
	"service-bus-hero/topics"
	"sync"
	"text/tabwriter"
	"time"
)

func GetConnectionString() {
	if appContext.ConnectionString != "" {
		return
	}

	connStr, err := prompts.PromptConnectionString()

	if err != nil {
		log.Fatalf("Failed to get connection string: %v", err)
		os.Exit(1)
	}

	appContext.ConnectionString = connStr
}

func SelectTopic() error {
	allTopics, err := topics.FetchTopics(appContext.ConnectionString)

	if err != nil {
		return fmt.Errorf("could not fetch topics: %w", err)
	}

	selectedTopic, err := prompts.PromptSelectTopic(allTopics)
	if err != nil {
		return fmt.Errorf("could not select topic: %w", err)
	}

	appContext.Topic = selectedTopic

	return nil
}

func SelectSubscription() error {
	allSubscriptions, err := topics.FetchTopicSubscriptions(appContext.ConnectionString, appContext.Topic)
	if err != nil {
		return fmt.Errorf("could not fetch subscriptions: %w", err)
	}

	selectedSubscription, err := prompts.PromptSelectSubscription(allSubscriptions)
	if err != nil {
		return fmt.Errorf("could not select subscription: %w", err)
	}

	appContext.Subscription = selectedSubscription

	return nil
}

func ListTopicStatByTopics() error {
	allTopics, err := topics.FetchTopics(appContext.ConnectionString)
	if err != nil {
		return fmt.Errorf("could not fetch topics: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 4, '\t', 0)
	fmt.Fprintln(w, "Topic\tSubscription\tActive Messages\tDLQ Messages\t")

	for _, topic := range allTopics {
		err = WriteTopicSubscriptionsStats(w, topic, false)
		if err != nil {
			return fmt.Errorf("could not write topic subscription stats: %w", err)
		}
	}

	// Ensure all data is flushed to standard output
	if err := w.Flush(); err != nil {
		return fmt.Errorf("could not flush writer: %w", err)
	}

	return nil
}

func ListDLQStats() error {
	allTopics, err := topics.FetchTopics(appContext.ConnectionString)
	if err != nil {
		return fmt.Errorf("could not fetch topics: %w", err)
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 8, 4, '\t', 0)
	fmt.Fprintln(w, "Topic\tSubscription\tActive Messages\tDLQ Messages\t")

	for _, topic := range allTopics {
		err = WriteTopicSubscriptionsStats(w, topic, true)
		if err != nil {
			return fmt.Errorf("could not write topic subscription stats: %w", err)
		}
	}

	// Ensure all data is flushed to standard output
	if err := w.Flush(); err != nil {
		return fmt.Errorf("could not flush writer: %w", err)
	}

	return nil
}

func WriteTopicSubscriptionsStats(w *tabwriter.Writer, topic string, dlqOnly bool) error {
	allSubscriptions, err := topics.FetchTopicSubscriptions(appContext.ConnectionString, topic)
	if err != nil {
		return fmt.Errorf("could not fetch subscriptions: %w", err)
	}

	for _, subscription := range allSubscriptions {
		subscriptionStats, err := topics.FetchTopicSubscriptionStats(appContext.ConnectionString, topic, subscription)
		if err != nil {
			return fmt.Errorf("could not fetch subscription stat: %w", err)
		}

		if dlqOnly && subscriptionStats.DeadLetterMessageCount == 0 {
			continue
		}

		// Write each subscription's stats in a row
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t\n", topic, subscription, subscriptionStats.ActiveMessageCount, subscriptionStats.DeadLetterMessageCount)
	}

	return nil
}

func WriteDLQMessagesToFile(receiveMode azservicebus.ReceiveMode) error {
	currentTime := time.Now()
	timestamp := currentTime.Format("20060102-150405")

	if appContext.Topic == "" {
		err := SelectTopic()
		if err != nil {
			return fmt.Errorf("could not select topic: %w", err)
		}
	}

	if appContext.Subscription == "" {
		err := SelectSubscription()
		if err != nil {
			return fmt.Errorf("could not select subscription: %w", err)
		}
	}

	defaultFileName := fmt.Sprintf("%s-%s-%s-dlq-messages.jsonl", appContext.Topic, appContext.Subscription, timestamp)

	fileName, err := prompts.PromptFileName(&defaultFileName)
	if err != nil {
		return fmt.Errorf("could not get file name: %w", err)
	}

	messageChan, errChan := topics.FetchDLQMessages(appContext.ConnectionString, appContext.Topic, appContext.Subscription, receiveMode)

	var wg sync.WaitGroup
	var totalMessages int

	wg.Add(1)
	go func(totalMessagesPtr *int) {
		defer wg.Done()
		total, err := io.WriteMessagesToJsonLinesFile(messageChan, fileName)
		if err != nil {
			fmt.Printf("Error writing messages to file: %v\n", err)
			return
		}

		*totalMessagesPtr = total
	}(&totalMessages)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errChan {
			if err != nil {
				fmt.Printf("Error processing messages: %v\n", err)
				// Handle the error as needed
			}
		}
	}()

	wg.Wait()

	fmt.Printf("%d messages written to file: %s\n", totalMessages, fileName)

	return nil
}

func ResendAllDLQMessages() error {
	allTopics, err := topics.FetchTopics(appContext.ConnectionString)
	if err != nil {
		return fmt.Errorf("could not fetch topics: %w", err)
	}

	totalResent := 0

	for _, topic := range allTopics {
		subscriptions, err := topics.FetchTopicSubscriptions(appContext.ConnectionString, topic)
		if err != nil {
			fmt.Printf("Error fetching subscriptions for topic %s: %v\n", topic, err)
			continue
		}

		for _, subscription := range subscriptions {
			stats, err := topics.FetchTopicSubscriptionStats(appContext.ConnectionString, topic, subscription)
			if err != nil {
				fmt.Printf("Error fetching stats for %s/%s: %v\n", topic, subscription, err)
				continue
			}

			if stats.DeadLetterMessageCount == 0 {
				continue
			}

			fmt.Printf("Resending %d DLQ messages from %s/%s...\n", stats.DeadLetterMessageCount, topic, subscription)

			count, err := topics.ResendDLQMessages(appContext.ConnectionString, topic, subscription)
			if err != nil {
				fmt.Printf("Error resending DLQ messages for %s/%s: %v\n", topic, subscription, err)
				continue
			}

			totalResent += count
			fmt.Printf("Resent %d messages from %s/%s\n", count, topic, subscription)
		}
	}

	fmt.Printf("\nTotal messages resent: %d\n", totalResent)
	return nil
}

func ClearAllDLQMessages() error {
	allTopics, err := topics.FetchTopics(appContext.ConnectionString)
	if err != nil {
		return fmt.Errorf("could not fetch topics: %w", err)
	}

	totalCleared := 0

	for _, topic := range allTopics {
		subscriptions, err := topics.FetchTopicSubscriptions(appContext.ConnectionString, topic)
		if err != nil {
			fmt.Printf("Error fetching subscriptions for topic %s: %v\n", topic, err)
			continue
		}

		for _, subscription := range subscriptions {
			stats, err := topics.FetchTopicSubscriptionStats(appContext.ConnectionString, topic, subscription)
			if err != nil {
				fmt.Printf("Error fetching stats for %s/%s: %v\n", topic, subscription, err)
				continue
			}

			if stats.DeadLetterMessageCount == 0 {
				continue
			}

			fmt.Printf("Clearing %d DLQ messages from %s/%s...\n", stats.DeadLetterMessageCount, topic, subscription)

			count, err := topics.ClearDLQMessages(appContext.ConnectionString, topic, subscription)
			if err != nil {
				fmt.Printf("Error clearing DLQ messages for %s/%s: %v\n", topic, subscription, err)
				continue
			}

			totalCleared += count
			fmt.Printf("Cleared %d messages from %s/%s\n", count, topic, subscription)
		}
	}

	fmt.Printf("\nTotal messages cleared: %d\n", totalCleared)
	return nil
}

func PublishMessages() error {
	var err error
	var wg sync.WaitGroup

	if appContext.Topic == "" {
		err = SelectTopic()
		if err != nil {
			return fmt.Errorf("could not select topic: %w", err)
		}
	}

	existingFiles, err := io.ListJsonlFiles()
	if err != nil {
		return fmt.Errorf("could not list existing files: %w", err)
	}

	fileName := ""

	if len(existingFiles) == 0 {
		fileName, err = prompts.EnterCustomFileName()
	} else {
		fileName, err = prompts.SelectFileOrCustom(existingFiles)
	}

	messagesChan, errChan := io.ReadMessagesFromJsonLinesFile(fileName)
	azMessagesChan := make(chan *azservicebus.Message)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(azMessagesChan)

		for msg := range messagesChan {
			azMsg := io.TransformMessage(msg)
			azMessagesChan <- azMsg
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errChan {
			// Handle or log the error. Break if necessary.
			fmt.Println("Error from GenerateDataA:", err)
			// Optionally, set an external error or handle it as needed.
			// This example just prints the error.
		}
	}()

	err = topics.PublishMessagesToTopic(appContext.ConnectionString, appContext.Topic, azMessagesChan)

	wg.Wait()

	return err
}
