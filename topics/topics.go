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

func GetDLQMessageCount(connStr string, topic string, subscription string) (int, error) {
	client, err := admin.NewClientFromConnectionString(connStr, nil)
	if err != nil {
		return 0, fmt.Errorf("could not create service bus admin client: %w", err)
	}

	ctx := context.Background()
	subscriptionProps, err := client.GetSubscriptionRuntimeProperties(ctx, topic, subscription, nil)
	if err != nil {
		return 0, fmt.Errorf("could not fetch subscription runtime properties: %w", err)
	}

	return int(subscriptionProps.DeadLetterMessageCount), nil
}

func FetchDLQMessages(connStr string, topic string, subscription string, receiveMode azservicebus.ReceiveMode) (<-chan *azservicebus.ReceivedMessage, <-chan error) {
	messageChan := make(chan *azservicebus.ReceivedMessage)
	errorChan := make(chan error, 1) // Buffered channel for at most one error

	go func() {
		defer close(messageChan)
		defer close(errorChan)

		client, err := azservicebus.NewClientFromConnectionString(connStr, nil)
		if err != nil {
			errorChan <- fmt.Errorf("could not create service bus client: %w", err)
			return
		}

		receiver, err := client.NewReceiverForSubscription(
			topic,
			subscription,
			&azservicebus.ReceiverOptions{
				SubQueue:    azservicebus.SubQueueDeadLetter,
				ReceiveMode: receiveMode,
			},
		)
		if err != nil {
			errorChan <- fmt.Errorf("could not create receiver for DLQ: %w", err)
			return
		}
		defer receiver.Close(context.Background())

		ctx := context.Background()

		dlqMessageCount, err := GetDLQMessageCount(connStr, topic, subscription)
		if err != nil {
			errorChan <- fmt.Errorf("could not fetch DLQ message count: %w", err)
			return
		}

		fmt.Printf("Found %d messages in DLQ\n", dlqMessageCount)

		downloadCount := 0
		maxBatchSize := 25

		// Now attempt to receive that many messages. Understand that the actual number may vary.
		for downloadCount < dlqMessageCount {
			receivedMessages, err := receiver.ReceiveMessages(ctx, maxBatchSize, &azservicebus.ReceiveMessagesOptions{})
			if err != nil {
				errorChan <- fmt.Errorf("could not receive messages from DLQ: %w", err)
				return
			}

			downloadCount += len(receivedMessages)

			for _, msg := range receivedMessages {
				messageChan <- msg
			}

			fmt.Printf("Downloaded %d messages\n", downloadCount)

			if len(receivedMessages) == 0 {
				break
			}
		}
	}()

	return messageChan, errorChan
}

func PublishMessagesToTopic(connString string, topic string, messageChan <-chan *azservicebus.Message) error {
	client, err := azservicebus.NewClientFromConnectionString(connString, nil)
	if err != nil {
		return fmt.Errorf("could not create service bus client: %w", err)
	}

	sender, err := client.NewSender(topic, nil)
	if err != nil {
		return fmt.Errorf("could not create sender for topic: %w", err)
	}
	defer sender.Close(context.Background())

	ctx := context.Background()

	batch, err := sender.NewMessageBatch(ctx, &azservicebus.MessageBatchOptions{})
	if err != nil {
		return fmt.Errorf("could not create message batch: %w", err)
	}

	sentItems := 0

	for msg := range messageChan {
		if err := batch.AddMessage(msg, &azservicebus.AddMessageOptions{}); err != nil {
			return fmt.Errorf("could not add message to batch: %w", err)
		}

		if batch.NumMessages() == 100 {
			if err := sender.SendMessageBatch(ctx, batch, &azservicebus.SendMessageBatchOptions{}); err != nil {
				return fmt.Errorf("could not send message batch: %w", err)
			}

			sentItems += int(batch.NumMessages())
			fmt.Printf("Sent %d messages\n", sentItems)

			batch, err = sender.NewMessageBatch(ctx, &azservicebus.MessageBatchOptions{})
			if err != nil {
				return fmt.Errorf("could not create message batch: %w", err)
			}
		}
	}

	if batch.NumMessages() > 0 {
		if err := sender.SendMessageBatch(ctx, batch, &azservicebus.SendMessageBatchOptions{}); err != nil {
			return fmt.Errorf("could not send message batch: %w", err)
		}

		sentItems += int(batch.NumMessages())
		fmt.Printf("Sent %d messages\n", sentItems)
	}

	return nil

}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
