# kafka-js

A robust npm package for handling dynamic topic creation, subscription, and management in multi-instance microservice architectures. Built on top of KafkaJS, this package provides a simplified interface for Kafka operations while maintaining full flexibility and control.

## Features

- 🚀 **Dynamic Topic Management** - Automatic topic creation and subscription
- 🔄 **Multi-Instance Support** - Designed for microservice architectures
- 📝 **Topic Registry** - Easy topic registration and management
- 🎯 **Flexible Messaging** - Support for both batch and single message processing
- ⚡ **Built on KafkaJS** - Leverages the power and reliability of KafkaJS
- 🛠️ **TypeScript Support** - Full TypeScript definitions included
- 🌐 **Dynamic Topic Subscription** - Automatic subscription to related topics using pattern matching

## Installation

```bash
npm install kafka-js
```

## Important Setup Requirements

### 1. Create `Topic Updates` Topic (One-Time Setup)

**⚠️ IMPORTANT:** Before using kafka-js in your environment, you must create a special topic named `topic-updates` once. This topic facilitates dynamic topic subscription for all consumers.

```bash
# Create the topic-updates topic (one-time setup per environment)
kafka-topics.sh --create --topic topic-updates --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

This topic is used internally by kafka-js to manage dynamic topic registration and subscription across all microservice instances.

### 2. Topic Naming Convention

kafka-js uses a specific topic naming convention for dynamic subscription:

**Pattern:** `{env}-{serviceName}-{entityName}.{topicName}`

**Examples:**

- `dev-user-service-user123.events`
- `prod-order-service-order456.notifications`
- `staging-payment-service-payment789.transactions`

**How it works:**

- When you register a consumer with `topicName` (e.g., "events"), the consumer will automatically subscribe to ALL topics that end with `.events`
- This allows for dynamic topic creation and automatic subscription without manual intervention

### 3. Topic Naming Restrictions

**⚠️ WARNING:** Do NOT create topics with dots (`.`) in the base topic name when using standard topic creation.

**Incorrect:** ❌

```javascript
// Don't do this - dots should only be used in the naming convention
await kafka.producer.sendMessage("user.events.topic", message);
```

**Correct:** ✅

```javascript
// Use the naming convention or simple names without dots
await kafka.producer.sendMessage("user-events", message);
// or let kafka-js handle the naming convention internally
```

The dot (`.`) is reserved for the dynamic topic subscription pattern and should only appear before the final topic name segment.

## Quick Start

### 1. Create Kafka Client

```javascript
import { getKafkaClient } from "kafka-js";

const kafka = getKafkaClient({
  env: "dev",
  brokers: ["localhost:9092"],
  clientId: "kafka-default-service",
  serviceName: "user-service",
  partitions: 1, // optional
  replicationFactor: 1, // optional
  acks: 1, // optional
});
```

#### You can also pass option in getKafkaClient:-

```javascript
const kafka = getKafkaClient(
  {
    env: "dev",
    brokers: ["localhost:9092"],
    clientId: "kafka-default-service",
    serviceName: "user-service",
  },
  {
    isSingletonEnabled: boolean, // It will return only one instance if called multiple times by defalut it is true.
  }
);
```

### 2. Register Consumers (Before Init)

**Important:** Register all consumers before calling the `init()` method.

```javascript
// Register batch consumer - will subscribe to all topics ending with .user-events
kafka.registry.registerBatch("user-events", handleBatchMessages, {
  consumerGroup: "my-service-group",
  fromBeginning: false,
});

// Register single message consumer - will subscribe to all topics ending with .user-events
kafka.registry.registerSingle("user-events", handleSingleMessage, {
  consumerGroup: "my-service-group",
  fromBeginning: false,
});

// This is optional
{
  consumerGroup: "my-service-group",
  fromBeginning: false,
}
```

### 3. Initialize Client

```javascript
await kafka.init();
```

### 4. Send Messages

```javascript
const message = {
  key: "user-123",
  value: {
    userId: "123",
    action: "login",
    timestamp: new Date().toISOString(),
  },
};

await kafka.producer.sendMessage("user-events", message, "user-123", {
  acks: 1,
});
```

**Note:** How `acks` priority works.

By default, the producer is configured to acks the messages with the following logic:

1. If acks is mentioned in the sendMessage will have more priority than mentioned in the getkafkaClient

## Dynamic Topic Subscription

### How It Works

When you register a consumer with a topic name like `'user-events'`, kafka-js automatically:

1. Subscribes to all existing topics that match the pattern `*.user-events`
2. Monitors the `topic-updates` topic for new topic registrations
3. Dynamically subscribes to new topics that match the pattern as they are created

### Example Scenario

```javascript
// Consumer registration
kafka.registry.registerBatch("notifications", handleNotifications, {
  consumerGroup: "notification-consumer-group",
});

// This consumer will automatically receive messages from ALL of these topics:
// - dev-user-service-user123.notifications
// - dev-order-service-order456.notifications
// - dev-payment-service-payment789.notifications
// - Any future topics ending with .notifications
```

### Topic Creation Examples

```javascript
// These will create topics following the naming convention:
await kafka.producer.sendMessage("user-events", message, "user123");
// Creates: {env}-{serviceName}-user123.user-events

await kafka.producer.sendMessage("order-updates", message, "order456");
// Creates: {env}-{serviceName}-order456.order-updates

await kafka.producer.sendMessage("payments", message, "payment789");
// Creates: {env}-{serviceName}-payment789.payments
```

#### Note:- When you sendMessage if the topic is not created sendMessage will automatically handle the new topic creation and topic-updates will handle the topic registration.

## Graceful shutdown

### How It Works

Ensure proper cleanup when shutting down your application. This will disconnect all the consumers, producers and admin connections.

```javascript
await kafka.shutdown();
```

<br>

## API Reference

### Client Configuration

#### `getkafkaClient(config)`

Creates a new Kafka client instance.

**Parameters:**

| Parameter           | Type       | Required | Description                                     |
| ------------------- | ---------- | -------- | ----------------------------------------------- |
| `env`               | `string`   | Yes      | Environment (e.g., 'development', 'production') |
| `brokers`           | `string[]` | Yes      | Array of Kafka broker addresses                 |
| `clientId`          | `string`   | Yes      | Unique client identifier                        |
| `serviceName`       | `string`   | Yes      | Name of your service used for topic creation    |
| `partitions`        | `number`   | No       | Default number of partitions for topics         |
| `replicationFactor` | `number`   | No       | Default replication factor for topics           |
| `acks`              | `number`   | No       | Default acknowledgment setting                  |

### Producer

#### `producer.sendMessage(topic, message, entityId, options)`

Sends a message to a specified topic.

**Parameters:**

- `topic` (string): The base topic name (without dots)
- `message` (MessagePayload): The message payload
- `entityId` (string): Entity identifier for partitioning and topic naming
- `options` (IKafkaProducerOptions): Producer options

**MessagePayload Interface:**

```typescript
interface MessagePayload {
  key?: string;
  value: any;
  timestamp?: string;
  partition?: number;
  headers?: IHeaders;
}
```

**IKafkaProducerOptions Interface:**

```typescript
interface IKafkaProducerOptions {
  acks?: number;
}
```

### Consumer Registry

#### `registry.registerBatch(topic, handler, options)`

Registers a batch message handler for a topic pattern.

**Parameters:**

- `topic` (string): Base topic name to subscribe to (will match all topics ending with `.{topic}`)
- `handler` (function): Callback function to handle messages
- `options` (ITopicRegistryOptions): Consumer options

**Handler Signature:**

```typescript
async function handleBatchMessages(params: { topic: string; messages: any[] }) {
  // Handle batch of messages
  // topic will be the full topic name (e.g., 'dev-user-service-user123.events')
}
```

#### `registry.registerSingle(topic, handler, options)`

Registers a single message handler for a topic pattern.

**Parameters:**

- `topic` (string): Base topic name to subscribe to (will match all topics ending with `.{topic}`)
- `handler` (function): Callback function to handle individual messages
- `options` (ITopicRegistryOptions): Consumer options

**Handler Signature:**

```typescript
async function handleSingleMessage(params: {
  topic: string;
  message: any;
  partition: number;
  offset: string;
}) {
  // Handle single message
  // topic will be the full topic name (e.g., 'dev-user-service-user123.events')
}
```

## Complete Example

```javascript
import { getkafkaClient } from "kafka-js";

class KafkaService {
  constructor() {
    this._kafka = getkafkaClient({
      env: "dev", // e.g., 'dev'
      brokers: ["localhost:9092"],
      clientId: "kafka-default-service",
      serviceName: "user-service", // e.g., 'user-service'
      partitions: 1,
      replicationFactor: 1,
    });
  }

  async initialize() {
    // Register consumers before init
    // This will subscribe to all topics ending with .user-events
    this._kafka.registry.registerBatch(
      "user-events",
      this.handleUserEvents.bind(this),
      {
        consumerGroup: "user-service-group",
        fromBeginning: false,
      }
    );

    // This will subscribe to all topics ending with .notifications
    this._kafka.registry.registerSingle(
      "notifications",
      this.handleNotification.bind(this),
      {
        consumerGroup: "notification-service-group",
        fromBeginning: true,
      }
    );

    // Initialize the client
    await this._kafka.init();
    console.log("Kafka client initialized");
  }

  // Batch message function
  async handleUserEvents(params) {
    const { topic, messages } = params;
    console.log(
      `[Kafka][Batch][${topic}] Received ${messages.length} messages`
    );

    for (const message of messages) {
      console.log(`Processing message for topic ${topic}:`, message);
    }
  }

  // Single message function
  async handleNotification(params) {
    const { topic, message, partition, offset } = params;
    console.log(
      `[Kafka][Single][${topic}] Message from partition ${partition}, offset ${offset}`
    );
    console.log("Message:", message);
  }

  async sendUserEvent(userId, eventData) {
    const message = {
      key: userId,
      value: {
        userId,
        ...eventData,
        timestamp: new Date().toISOString(),
      },
      headers: {
        source: "user-service",
      },
    };

    // This will create topic: dev-user-service-user123.user-events if not present and send the message to the topic created
    await this._kafka.producer.sendMessage("user-events", message, userId);
  }

  async sendNotification(userId, notificationData) {
    const message = {
      key: userId,
      value: notificationData,
    };

    // This will create topic: dev-user-service-user123.notifications if not present and send the message to the topic created
    await this._kafka.producer.sendMessage("notifications", message, userId);
  }
}

// Usage
const kafkaService = new KafkaService();
await kafkaService.initialize();

// Send messages that will be automatically routed to appropriate consumers
await kafkaService.sendUserEvent("user-123", {
  action: "profile_updated",
  data: { email: "user@example.com" },
});

await kafkaService.sendNotification("user-123", {
  type: "email",
  subject: "Profile Updated",
  body: "Your profile has been successfully updated.",
});
```

<br>

## Best Practices

1. **One-Time Setup**: Ensure the `topic-updates` topic is created before deploying any services
2. **Register Consumers First**: Always register all consumers before calling `init()`
3. **Avoid Dots in Topic Names**: Don't use dots (`.`) in your base topic names - let kafka-js handle the naming convention
4. **Use Meaningful Topic Names**: Choose descriptive topic names that reflect the data flow
5. **Consumer Groups**: Use appropriate consumer group names to ensure proper load balancing
6. **Entity IDs**: Use meaningful entity IDs as they become part of the topic name
7. **Error Handling**: Implement proper error handling in your message handlers
8. **Graceful Shutdown**: Ensure proper cleanup when shutting down your application

## Topic Naming Best Practices

### ✅ Good Topic Names

```javascript
await kafka.producer.sendMessage("user-events", message, entityId);
await kafka.producer.sendMessage("order-updates", message, entityId);
await kafka.producer.sendMessage("payment-transactions", message, entityId);
await kafka.producer.sendMessage("notifications", message, entityId);
```

### ❌ Avoid These Topic Names

```javascript
// Don't use dots in base topic names
await kafka.producer.sendMessage("user.events", message, entityId); // ❌
await kafka.producer.sendMessage("order.status.updates", message, entityId); // ❌

// These are reserved patterns
await kafka.producer.sendMessage("topic-updates", message, entityId); // ❌ Reserved
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License.

## Support

For issues and questions, please open an issue on the GitHub repository.
