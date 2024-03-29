package main

import (
	"fmt"
	"log"
	"os"
	"service-bus-hero/io"
	"service-bus-hero/prompts"
	"service-bus-hero/topics"
	"text/tabwriter"
	"time"
)

func GetConnectionString() {
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
		WriteTopicSubscriptionsStats(w, topic)
	}

	// Ensure all data is flushed to standard output
	if err := w.Flush(); err != nil {
		return fmt.Errorf("could not flush writer: %w", err)
	}

	return nil
}

func WriteTopicSubscriptionsStats(w *tabwriter.Writer, topic string) error {
	allSubscriptions, err := topics.FetchTopicSubscriptions(appContext.ConnectionString, topic)
	if err != nil {
		return fmt.Errorf("could not fetch subscriptions: %w", err)
	}

	for _, subscription := range allSubscriptions {
		subscriptionStats, err := topics.FetchTopicSubscriptionStats(appContext.ConnectionString, topic, subscription)
		if err != nil {
			return fmt.Errorf("could not fetch subscription stat: %w", err)
		}

		// Write each subscription's stats in a row
		fmt.Fprintf(w, "%s\t%s\t%d\t%d\t\n", topic, subscription, subscriptionStats.ActiveMessageCount, subscriptionStats.DeadLetterMessageCount)
	}

	return nil
}

func WriteDLQMessagesToFile() error {
	currentTime := time.Now()
	timestamp := currentTime.Format("20060102-150405")
	defaultFileName := fmt.Sprintf("%s-%s-%s-dlq-messages.jsonl", appContext.Topic, appContext.Subscription, timestamp)

	fileName, err := prompts.PromptFileName(&defaultFileName)
	if err != nil {
		return fmt.Errorf("could not get file name: %w", err)
	}

	messages, err := topics.FetchDLQMessages(appContext.ConnectionString, appContext.Topic, appContext.Subscription)

	if err != nil {
		return fmt.Errorf("could not fetch DLQ messages: %w", err)
	}

	if err := io.WriteMessagesToJsonFile(messages, fileName); err != nil {
		return fmt.Errorf("could not write messages to file: %w", err)
	}

	fmt.Printf("Messages written to file: %s\n", fileName)

	return nil
}
