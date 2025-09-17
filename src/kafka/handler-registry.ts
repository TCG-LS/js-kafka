/**
 * @fileoverview Topic handler registration and management
 * @description Provides a registry for registering message handlers for different topics
 */

import {
    BatchMessageHandler,
    ITopicRegistryOptions,
    KafkaConfig,
    SingleMessageHandler,
} from "../interface/kafka.interface";
import { Logger } from "../logger/logger";
import { KafkaConnectionManager } from "./connection-manager";
import { KafkaConsumerManager } from "./consumer-manager";

/**
 * Registry for managing topic handlers and consumer registration.
 * 
 * @description This class provides a simple interface for registering message handlers
 * for different topics. It supports both single message and batch message processing
 * patterns and integrates with the consumer manager for actual subscription management.
 * 
 * @example
 * ```typescript
 * const registry = new KafkaTopicHandlerRegistry(connection, config);
 * 
 * // Register single message handler
 * registry.registerSingle('user-events', async (params) => {
 *   console.log('Received message:', params.message);
 * });
 * 
 * // Register batch message handler
 * registry.registerBatch('notifications', async (params) => {
 *   console.log('Received batch:', params.messages.length);
 * });
 * ```
 */
export class KafkaTopicHandlerRegistry {
    private logger: Logger;
    private kafkaConsumer: KafkaConsumerManager;
    /**
     * Creates a new KafkaTopicHandlerRegistry instance.
     * 
     * @param kafkaConnection - Kafka connection manager
     * @param config - Kafka configuration
     */
    constructor(
        private readonly kafkaConnection: KafkaConnectionManager,
        config: KafkaConfig
    ) {
        this.logger = Logger.getInstance();
        this.kafkaConsumer = KafkaConsumerManager.getInstance(
            this.kafkaConnection,
            config
        );
    }

    /**
     * Registers a handler for processing individual messages.
     * 
     * @param topic - Base topic name to subscribe to (matches all topics ending with .{topic})
     * @param handler - Function to handle individual messages
     * @param options - Optional consumer configuration
     * 
     * @description Registers a handler that will process messages one at a time.
     * The handler will be called for each message received on topics matching
     * the pattern *.{topic}. This is suitable for low-latency processing.
     * 
     * @example
     * ```typescript
     * registry.registerSingle('user-events', async ({ topic, message, partition, offset }) => {
     *   console.log(`Processing message from ${topic}:`, message);
     *   // Process individual message
     * }, {
     *   consumerGroup: 'user-service-single',
     *   fromBeginning: false
     * });
     * ```
     */
    registerSingle(
        topic: string,
        handler: SingleMessageHandler,
        options?: ITopicRegistryOptions
    ) {
        this.logger.debug(`registerd [single][topic]: ${topic}`);
        this.kafkaConsumer.setSingleTopicHandler(topic, handler, options);
    }

    /**
     * Registers a handler for processing batches of messages.
     * 
     * @param topic - Base topic name to subscribe to (matches all topics ending with .{topic})
     * @param handler - Function to handle message batches
     * @param options - Optional consumer configuration
     * 
     * @description Registers a handler that will process messages in batches.
     * The handler will be called with arrays of messages for better throughput.
     * This is suitable for high-volume processing where batch operations are more efficient.
     * 
     * @example
     * ```typescript
     * registry.registerBatch('analytics-events', async ({ topic, messages, partition, offset }) => {
     *   console.log(`Processing ${messages.length} messages from ${topic}`);
     *   // Process batch of messages
     *   await processBatch(messages);
     * }, {
     *   consumerGroup: 'analytics-batch',
     *   maxBytes: 262144, // .25MB batches
     *   fromBeginning: true
     * });
     * ```
     */
    registerBatch(
        topic: string,
        handler: BatchMessageHandler,
        options?: ITopicRegistryOptions
    ) {
        this.logger.debug(`registerd [batch][topic]: ${topic}`);
        this.kafkaConsumer.setBatchTopicHandler(topic, handler, options);
    }
}
