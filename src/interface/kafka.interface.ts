/**
 * @fileoverview TypeScript interfaces and types for Kafka client
 * @description Defines all interfaces, types, and contracts used by the js-kafka library
 */

import { IHeaders, KafkaMessage } from 'kafkajs';

/**
 * Configuration interface for Kafka client initialization.
 * 
 * @description Defines all configuration options needed to initialize
 * a Kafka client instance. Required properties must be provided,
 * while optional properties have sensible defaults.
 */
export interface KafkaConfig {
    /** Environment name (e.g., 'dev', 'staging', 'production') */
    env: string;
    /** Array of Kafka broker addresses (e.g., ['localhost:9092']) */
    brokers: string[];
    /** Unique identifier for this Kafka client */
    clientId: string;
    /** Service name used in topic naming convention */
    serviceName: string;
    /** Default consumer group ID for consumers */
    consumerGroupId: string;
    /** Default number of partitions for new topics (default: 1) */
    partitions?: number;
    /** Default replication factor for new topics (default: 1) */
    replicationFactor?: number;
    /** Default acknowledgment setting for producers (default: 1) */
    acks?: number;
    /** Connection timeout in milliseconds (default: 3000) */
    connectionTimeout?: number;
}

/**
 * Message payload structure for sending messages to Kafka.
 * 
 * @description Defines the structure of messages that can be sent
 * through the producer. All fields except 'value' are optional.
 */
export interface MessagePayload {
    /** Optional message key for partitioning */
    key?: string;
    /** Message content (will be JSON stringified) */
    value: any;
    /** Optional timestamp (defaults to current time) */
    timestamp?: string;
    /** Optional partition number (auto-assigned if not specified) */
    partition?: number;
    /** Optional message headers */
    headers?: IHeaders;
}


/**
 * Handler function for processing individual messages.
 * 
 * @param param - Single message parameters
 * @returns Promise that resolves when message processing is complete
 */
export type SingleMessageHandler = (
    param: ISingleKafkaConsumerParams,
) => Promise<void>;

/**
 * Handler function for processing batches of messages.
 * 
 * @param params - Batch message parameters
 * @returns Promise that resolves when batch processing is complete
 */
export type BatchMessageHandler = (
    params: IBulkKafkaConsumerParam,
) => Promise<void>;

/**
 * Parameters passed to single message handlers.
 */
interface ISingleKafkaConsumerParams {
    /** Full topic name where the message was received */
    topic: string;
    /** The Kafka message object */
    message: KafkaMessage;
    /** Partition number where the message was received */
    partition?: number;
    /** Message offset in the partition */
    offset?: string;
}

/**
 * Parameters passed to batch message handlers.
 */
interface IBulkKafkaConsumerParam {
    /** Full topic name where the messages were received */
    topic: string;
    /** Array of Kafka message objects */
    messages: KafkaMessage[];
    /** Partition number where the messages were received */
    partition?: number;
    /** Last message offset in the batch */
    offset?: string;
}

/**
 * Options for topic registration and consumer configuration.
 * 
 * @description Allows customization of consumer behavior when registering
 * topic handlers. All options are optional and have sensible defaults.
 */
export interface ITopicRegistryOptions {
    /** Custom consumer group ID (defaults to service consumer group) */
    consumerGroup?: string;
    /** Whether to read from the beginning of the topic (default: false) */
    fromBeginning?: boolean;
    /** Maximum bytes to fetch per request (batch consumers only) */
    maxBytes?: number;
    /** Session timeout in milliseconds (default: 60000) */
    sessionTimeout?: number;
    /** Heartbeat interval in milliseconds (default: 30000) */
    heartbeatInterval?: number;
}

/**
 * Generic topic handler interface.
 */
export interface TopicHandler {
    /** Type of handler (single or batch) */
    type: 'single' | 'batch';
    /** The handler function */
    handler: SingleMessageHandler | BatchMessageHandler;
}

/**
 * Batch topic handler with configuration options.
 */
export interface BatchTopicMapHandler {
    /** Handler type identifier */
    type: 'batch';
    /** Batch message handler function */
    handler: BatchMessageHandler;
    /** Optional consumer configuration */
    options?: ITopicRegistryOptions;
}

/**
 * Single topic handler with configuration options.
 */
export interface SingleTopicMapHandler {
    /** Handler type identifier */
    type: 'single';
    /** Single message handler function */
    handler: SingleMessageHandler;
    /** Optional consumer configuration */
    options?: ITopicRegistryOptions;
}

/**
 * Parameters for the topic registry consumer (internal use).
 * 
 * @description Used internally for handling topic update notifications.
 */
export interface KafkaRegisteryConsumerParams {
    /** Topic name where the update was received */
    topic: string;
    /** The update message content */
    message: any;
    /** Partition number */
    partition?: number;
    /** Message offset */
    offset?: string;
}

/**
 * Kafka message structure (internal representation).
 */
export interface IKafkaMessage {
    /** Message key */
    key?: string
    /** Message value as string */
    value: string
    /** Message timestamp */
    timestamp?: string
    /** Message attributes */
    attributes?: number
    /** Message offset */
    offset?: string
    /** Message headers */
    headers?: IHeaders
    /** Size field (not used) */
    size?: never
}

/**
 * Consumer-specific configuration options.
 */
export interface IKafkaConsumerOptions {
    /** Maximum bytes to fetch per request */
    maxBytes?: number;
    /** Session timeout in milliseconds */
    sessionTimeout?: number;
    /** Heartbeat interval in milliseconds */
    heartbeatInterval?: number;
}

/**
 * Producer-specific configuration options.
 */
export interface IKafkaProducerOptions {
    /** Acknowledgment setting (-1, 0, or 1) */
    acks?: number;
}

/**
 * Options for KafkaClient creation behavior.
 */
export interface IGetKafkaClientOptions {
    /** Whether to use singleton pattern (default: true) */
    isSingletonEnabled?: boolean;
}
