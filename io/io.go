package io

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"os"
)

type SerializableMessage struct {
	Body                  string                 `json:"body"`
	Subject               string                 `json:"subject"`
	CorrelationID         string                 `json:"correlation_id"`
	ApplicationProperties map[string]interface{} `json:"application_properties"`
}

func WriteMessagesToJsonFile(messages []*azservicebus.ReceivedMessage, filename string) error {
	// Convert ReceivedMessage to SimplifiedMessage
	simplifiedMessages := make([]SerializableMessage, 0, len(messages))
	for _, msg := range messages {
		// Assume the message body is a string. Adjust the conversion based on your actual message body type.
		bodyText := string(msg.Body)

		message := SerializableMessage{
			Body:                  bodyText,
			ApplicationProperties: msg.ApplicationProperties,
		}

		if msg.Subject != nil {
			message.Subject = *msg.Subject
		}

		if msg.CorrelationID != nil {
			message.CorrelationID = *msg.CorrelationID
		}

		simplifiedMessages = append(simplifiedMessages, message)
	}

	// Open file for writing
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	// Create JSON encoder and set it to indent the output for readability
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")

	// Encode messages to JSON and write to file
	if err := encoder.Encode(simplifiedMessages); err != nil {
		return fmt.Errorf("failed to encode messages: %w", err)
	}

	return nil
}
