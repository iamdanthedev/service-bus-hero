package io

import (
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"os"
	"time"
)

type SerializableMessage struct {
	ApplicationProperties      map[string]interface{} `json:"applicationProperties"`
	Body                       string                 `json:"body"`
	ContentType                *string                `json:"contentType"`
	CorrelationID              *string                `json:"correlationID"`
	DeadLetterErrorDescription *string                `json:"deadLetterErrorDescription"`
	DeadLetterReason           *string                `json:"deadLetterReason"`
	DeadLetterSource           *string                `json:"deadLetterSource"`
	DeliveryCount              uint32                 `json:"deliveryCount"`
	EnqueuedSequenceNumber     *int64                 `json:"enqueuedSequenceNumber"`
	EnqueuedTime               *time.Time             `json:"enqueuedTime"`
	ExpiresAt                  *time.Time             `json:"expiresAt"`
	LockedUntil                *time.Time             `json:"lockedUntil"`
	LockToken                  [16]byte               `json:"lockToken"`
	MessageID                  string                 `json:"messageID"`
	PartitionKey               *string                `json:"partitionKey"`
	ReplyTo                    *string                `json:"replyTo"`
	ReplyToSessionID           *string                `json:"replyToSessionID"`
	ScheduledEnqueueTime       *time.Time             `json:"scheduledEnqueueTime"`
	SequenceNumber             *int64                 `json:"sequenceNumber"`
	SessionID                  *string                `json:"sessionID"`
	State                      string                 `json:"state"`
	Subject                    *string                `json:"subject"`
	TimeToLive                 *time.Duration         `json:"timeToLive"`
	To                         *string                `json:"to"`
}

func ListJsonlFiles() ([]string, error) {
	// Open the current directory
	dir, err := os.Open(".")
	if err != nil {
		return nil, fmt.Errorf("failed to open directory: %w", err)
	}
	defer dir.Close()

	// Read the names of all files in the directory
	fileInfos, err := dir.Readdir(0)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory: %w", err)
	}

	// Filter out only the files with the .jsonl extension
	var jsonlFiles []string
	for _, fileInfo := range fileInfos {
		if !fileInfo.IsDir() && len(fileInfo.Name()) > 6 && fileInfo.Name()[len(fileInfo.Name())-6:] == ".jsonl" {
			jsonlFiles = append(jsonlFiles, fileInfo.Name())
		}
	}

	return jsonlFiles, nil
}

func WriteMessagesToJsonLinesFile(messagesChan <-chan *azservicebus.ReceivedMessage, filename string) (int, error) {
	// Create parent directories if they don't exist
	dir := ""
	lastSlashIndex := -1
	for i := len(filename) - 1; i >= 0; i-- {
		if filename[i] == '/' || filename[i] == '\\' {
			lastSlashIndex = i
			break
		}
	}

	if lastSlashIndex != -1 {
		dir = filename[:lastSlashIndex]
		if err := os.MkdirAll(dir, 0755); err != nil {
			return 0, fmt.Errorf("failed to create directories: %w", err)
		}
	}

	// Open file for writing
	file, err := os.Create(filename)
	if err != nil {
		return 0, fmt.Errorf("failed to create file: %w", err)
	}
	defer file.Close()

	i := 0

	for receivedMsg := range messagesChan {
		i++

		// Convert each ReceivedMessage to a SerializableMessage
		message := SerializableMessage{
			ApplicationProperties:      receivedMsg.ApplicationProperties,
			Body:                       string(receivedMsg.Body),
			ContentType:                receivedMsg.ContentType,
			CorrelationID:              receivedMsg.CorrelationID,
			DeadLetterErrorDescription: receivedMsg.DeadLetterErrorDescription,
			DeadLetterReason:           receivedMsg.DeadLetterReason,
			DeadLetterSource:           receivedMsg.DeadLetterSource,
			DeliveryCount:              receivedMsg.DeliveryCount,
			EnqueuedSequenceNumber:     receivedMsg.EnqueuedSequenceNumber,
			EnqueuedTime:               receivedMsg.EnqueuedTime,
			ExpiresAt:                  receivedMsg.ExpiresAt,
			LockedUntil:                receivedMsg.LockedUntil,
			LockToken:                  receivedMsg.LockToken,
			MessageID:                  receivedMsg.MessageID,
			PartitionKey:               receivedMsg.PartitionKey,
			ReplyTo:                    receivedMsg.ReplyTo,
			ReplyToSessionID:           receivedMsg.ReplyToSessionID,
			ScheduledEnqueueTime:       receivedMsg.ScheduledEnqueueTime,
			SequenceNumber:             receivedMsg.SequenceNumber,
			SessionID:                  receivedMsg.SessionID,
			State:                      stateToString(int(receivedMsg.State)),
			Subject:                    receivedMsg.Subject,
			TimeToLive:                 receivedMsg.TimeToLive,
			To:                         receivedMsg.To,
		}

		// Serialize the SerializableMessage to JSON
		jsonBytes, err := json.Marshal(message)
		if err != nil {
			return 0, fmt.Errorf("failed to serialize message: %w", err)
		}

		// Write the JSON bytes to the file, followed by a newline to separate JSON objects
		if _, err := file.Write(jsonBytes); err != nil {
			return 0, fmt.Errorf("failed to write message to file: %w", err)
		}
		if _, err := file.WriteString("\n"); err != nil {
			return 0, fmt.Errorf("failed to write newline to file: %w", err)
		}
	}

	return i, nil
}

func ReadMessagesFromJsonLinesFile(filename string) (<-chan *SerializableMessage, <-chan error) {
	// Open file for reading
	file, err := os.Open(filename)
	if err != nil {
		errorChan := make(chan error)
		errorChan <- fmt.Errorf("failed to open file: %w", err)
		close(errorChan)
		return nil, errorChan
	}

	// Create a channel for the messages and errors
	messageChan := make(chan *SerializableMessage)
	errorChan := make(chan error)

	// Read the file line by line
	go func() {
		defer close(messageChan)
		defer close(errorChan)

		decoder := json.NewDecoder(file)
		for {
			var message SerializableMessage
			if err := decoder.Decode(&message); err != nil {
				if err.Error() == "EOF" {
					break
				}
				errorChan <- fmt.Errorf("failed to decode message: %w", err)
				return
			}
			messageChan <- &message
		}
	}()

	return messageChan, errorChan
}

func TransformMessage(msg *SerializableMessage) *azservicebus.Message {
	// Convert the SerializableMessage to a Message
	message := azservicebus.Message{
		Body: []byte(msg.Body),
	}

	if msg.Subject != nil && *msg.Subject != "" {
		message.Subject = msg.Subject
	}

	if msg.CorrelationID != nil && *msg.CorrelationID != "" {
		message.CorrelationID = msg.CorrelationID
	}

	if msg.ApplicationProperties != nil {
		message.ApplicationProperties = msg.ApplicationProperties
	}

	return &message
}

func stateToString(state int) string {
	switch state {
	case 0:
		return "Active"
	case 1:
		return "Deferred"
	case 2:
		return "Scheduled"
	default:
		return "Unknown"
	}
}
