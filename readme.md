# Service Bus Hero

A tool for monitoring and managing messages in Azure Service Bus namespace.

## Getting Started

### Prerequisites

- Go 1.21 or later
- Azure Service Bus namespace with appropriate access

### Installation

1. Clone the repository:
   ```
   git clone https://github.com/yourusername/service-bus-hero.git
   cd service-bus-hero
   ```

2. Install dependencies:
   ```
   go mod download
   ```

3. Build the application:
   ```
   go build -o sbhero
   ```

### Configuration

Create a `.env` file in the project root with the following variables:

```
SBHERO_CONNECTION_STRING=Endpoint=sb://<your-namespace>.servicebus.windows.net/;SharedAccessKeyName=<key-name>;SharedAccessKey=<key>
SBHERO_TOPIC=<default-topic-name>
```

### Running the Application

Run the compiled binary:
```
./sbhero
```

Or run directly with Go:
```
go run .
```

## Features

- Connection options
  - Connection string
  - Shared access key
  - Managed identity
  - Service principal
  - AZ CLI

- Connect to Azure Service Bus namespace
  - List all queues, topics, and subscriptions
- Monitor messages in queues and subscriptions
  - Peek messages to file
    - `azsb peek -q queue-name -f file-name`
  - Receive messages
  - Send messages
  - Delete messages
- Send messages to queues and topics
  - Send messages to queue
    - `azsb send -q queue-name -m message`
  - Send messages to topic
    - `azsb send -t topic-name -m message`
