package topics

import (
	"context"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus/admin"
)

func FetchTopics(connStr string) ([]string, error) {
	client, err := admin.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create service bus admin client: %w", err)
	}

	ctx := context.Background()
	pager := client.NewListTopicsPager(nil)

	var topics []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("could not fetch topics page: %w", err)
		}
		for _, topic := range page.Topics {
			topics = append(topics, topic.TopicName)
		}
	}

	return topics, nil
}

func FetchTopicStats(connStr string, topic string) (*admin.TopicRuntimeProperties, error) {
	client, err := admin.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create service bus admin client: %w", err)
	}

	ctx := context.Background()
	topicProps, err := client.GetTopicRuntimeProperties(ctx, topic, nil)
	if err != nil {
		return nil, fmt.Errorf("could not fetch topic runtime properties: %w", err)
	}

	return &topicProps.TopicRuntimeProperties, nil
}

func FetchTopicSubscriptions(connStr string, topic string) ([]string, error) {
	client, err := admin.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create service bus admin client: %w", err)
	}

	ctx := context.Background()
	pager := client.NewListSubscriptionsPager(topic, nil)

	var subscriptions []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("could not fetch subscriptions page: %w", err)
		}
		for _, subscription := range page.Subscriptions {
			subscriptions = append(subscriptions, subscription.SubscriptionName)
		}
	}

	return subscriptions, nil
}

func FetchTopicSubscriptionStats(connStr string, topic string, subscription string) (*admin.SubscriptionRuntimeProperties, error) {
	client, err := admin.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create service bus admin client: %w", err)
	}

	ctx := context.Background()
	subscriptionProps, err := client.GetSubscriptionRuntimeProperties(ctx, topic, subscription, nil)
	if err != nil {
		return nil, fmt.Errorf("could not fetch subscription runtime properties: %w", err)
	}

	return &subscriptionProps.SubscriptionRuntimeProperties, nil
}

func FetchDLQMessages(connStr string, topic string, subscription string) ([]*azservicebus.ReceivedMessage, error) {
	client, err := azservicebus.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return nil, fmt.Errorf("could not create service bus client: %w", err)
	}

	// Create a receiver for the DLQ. Note: Ensure to use the correct path for the DLQ.
	// The DLQ path typically follows the format "<topic-name>/Subscriptions/<subscription-name>/$DeadLetterQueue"
	receiver, err := client.NewReceiverForSubscription(
		topic,
		subscription,
		&azservicebus.ReceiverOptions{
			SubQueue:    azservicebus.SubQueueDeadLetter,
			ReceiveMode: azservicebus.ReceiveModePeekLock,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("could not create receiver for DLQ: %w", err)
	}
	defer receiver.Close(context.Background()) // Ensure the receiver is closed after usage

	var messages []*azservicebus.ReceivedMessage
	ctx := context.Background()

	// Attempt to receive messages. Adjust the number of messages and the maximum wait time as needed.
	receivedMessages, err := receiver.ReceiveMessages(ctx, 10, nil)
	if err != nil {
		return nil, fmt.Errorf("could not receive messages from DLQ: %w", err)
	}

	messages = append(messages, receivedMessages...)

	return messages, nil
}
