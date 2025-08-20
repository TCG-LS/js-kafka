/**
 * @fileoverview Main entry point for js-kafka library
 * @description Provides a comprehensive Kafka client for dynamic topic management,
 * message production, and consumption in microservice architectures.
 * 
 * @author js-kafka Team
 * @version 1.0.0
 */

import { KafkaConfigManager } from "./config/kafka-config";
import { DefaultTopics } from "./enums/kafka.enums";
import {
    IGetKafkaClientOptions,
    KafkaConfig,
} from "./interface/kafka.interface";
import { KafkaAdminManager } from "./kafka/admin-manager";
import { KafkaConnectionManager } from "./kafka/connection-manager";
import { KafkaConsumerManager } from "./kafka/consumer-manager";
import { KafkaTopicHandlerRegistry } from "./kafka/handler-registry";
import { KafkaProducerManager } from "./kafka/producer-manager";
import { Logger } from "./logger/logger";

/**
 * Main Kafka client class that orchestrates all Kafka operations.
 * 
 * @description This class provides a unified interface for Kafka operations including:
 * - Dynamic topic creation and management
 * - Message production with automatic topic creation
 * - Consumer registration and management
 * - Graceful shutdown and cleanup
 * 
 * @example
 * ```typescript
 * const kafka = new KafkaClient({
 *   env: 'dev',
 *   brokers: ['localhost:9092'],
 *   clientId: 'my-service',
 *   serviceName: 'user-service',
 *   consumerGroupId: 'user-service-group'
 * });
 * 
 * await kafka.init();
 * ```
 */
export class KafkaClient {
    private readonly config: KafkaConfig;
    private readonly logger: Logger;
    private readonly connection: KafkaConnectionManager;
    private readonly consumer: KafkaConsumerManager;
    public readonly producer: KafkaProducerManager;
    private readonly admin: KafkaAdminManager;
    public readonly registry: KafkaTopicHandlerRegistry;

    private isInitialized = false;
    private static instance: KafkaClient | null = null;

    /**
     * Creates a new KafkaClient instance.
     * 
     * @param kafkaConfig - Configuration object for Kafka client
     * @description Initializes all Kafka components and sets up circular dependencies.
     * The configuration is validated and merged with defaults.
     * 
     * @example
     * ```typescript
     * const client = new KafkaClient({
     *   env: 'production',
     *   brokers: ['kafka1:9092', 'kafka2:9092'],
     *   clientId: 'payment-service',
     *   serviceName: 'payment-service',
     *   consumerGroupId: 'payment-consumers',
     *   partitions: 3,
     *   replicationFactor: 2
     * });
     * ```
     */
    constructor(kafkaConfig: KafkaConfig) {
        this.logger = Logger.getInstance();

        this.logger.debug(`Kafka Config === ${JSON.stringify(kafkaConfig)}`)


        // Load merged config
        this.config = KafkaConfigManager.loadConfig(kafkaConfig);


        // Core components
        this.connection = KafkaConnectionManager.getInstance(this.config);
        this.consumer = KafkaConsumerManager.getInstance(
            this.connection,
            this.config
        );
        this.admin = KafkaAdminManager.getInstance(this.connection, this.config);
        this.producer = KafkaProducerManager.getInstance(
            this.connection,
            this.config
        );
        this.registry = new KafkaTopicHandlerRegistry(this.connection, this.config);

        // Circular reference resolution
        this.consumer.setAdmin(this.admin);
        this.producer.setAdmin(this.admin);
        this.admin.setProducer(this.producer);
    }

    /**
     * Gets or creates a singleton instance of KafkaClient.
     * 
     * @param kafkaConfig - Configuration object for Kafka client
     * @returns The singleton KafkaClient instance
     * 
     * @description Implements the singleton pattern to ensure only one KafkaClient
     * instance exists per application. Subsequent calls return the same instance.
     * 
     * @example
     * ```typescript
     * const kafka1 = KafkaClient.getInstance(config);
     * const kafka2 = KafkaClient.getInstance(config);
     * console.log(kafka1 === kafka2); // true
     * ```
     */
    public static getInstance(kafkaConfig: KafkaConfig): KafkaClient {
        if (!KafkaClient.instance) {
            KafkaClient.instance = new KafkaClient(kafkaConfig);
        }
        return KafkaClient.instance;
    }

    /**
     * Initializes the Kafka client and all its components.
     * 
     * @returns Promise that resolves when initialization is complete
     * @throws Error if initialization fails
     * 
     * @description This method must be called after registering all consumers
     * and before sending any messages. It performs the following operations:
     * 1. Registers the topic-updates handler for dynamic subscription
     * 2. Populates the topic map with existing topics
     * 3. Initializes all consumers
     * 4. Sets the initialized flag
     * 
     * @example
     * ```typescript
     * // Register consumers first
     * kafka.registry.registerSingle('user-events', handleUserEvents);
     * 
     * // Then initialize
     * await kafka.init();
     * 
     * // Now ready to send messages
     * await kafka.producer.sendMessage('user-events', message, 'user123');
     * ```
     */
    async init() {
        if (this.isInitialized) {
            this.logger.warn("KafkaClient is already initialized");
            return;
        }
        this.registry.registerSingle(
            DefaultTopics.TOPIC_UPDATES,
            this.consumer.handleTopicUpdatesEvents.bind(this)
        );

        await this.admin.populateTopicMapOnStart();
        await this.consumer.initConsumer();

        this.isInitialized = true;
        this.logger.info(`Kafka initialized successfully!!`);
    }

    /**
     * Performs graceful shutdown of the Kafka client.
     * 
     * @returns Promise that resolves when shutdown is complete
     * 
     * @description Cleanly shuts down all Kafka connections including:
     * - All consumer connections
     * - All producer connections  
     * - Admin client connections
     * - Connection manager cleanup
     * 
     * This method should be called when the application is shutting down
     * to ensure proper cleanup and prevent resource leaks.
     * 
     * @example
     * ```typescript
     * process.on('SIGTERM', async () => {
     *   await kafka.shutdown();
     *   process.exit(0);
     * });
     * ```
     */
    async shutdown(): Promise<void> {
        this.logger.warn("Shutting down KafkaClient gracefully...");

        try {
            // Close connection
            await this.connection.shutdown();

            this.logger.info("✅ KafkaClient shutdown completed.");
        } catch (error) {
            this.logger.error("❌ Error during KafkaClient shutdown:", error);
        }
    }
}

/**
 * Factory function to create or get a KafkaClient instance.
 * 
 * @param kafkaConfig - Configuration object for Kafka client
 * @param options - Optional configuration for client creation behavior
 * @returns KafkaClient instance (singleton or new instance based on options)
 * 
 * @description This is the recommended way to create a KafkaClient instance.
 * By default, it returns a singleton instance, but can be configured to create
 * new instances for testing or special use cases.
 * 
 * @example
 * ```typescript
 * // Get singleton instance (default behavior)
 * const kafka = getKafkaClient(config);
 * 
 * // Create new instance for testing
 * const testKafka = getKafkaClient(config, { isSingletonEnabled: false });
 * 
 * // Explicit singleton
 * const singletonKafka = getKafkaClient(config, { isSingletonEnabled: true });
 * ```
 */
export function getKafkaClient(
    kafkaConfig: KafkaConfig,
    options?: IGetKafkaClientOptions
): KafkaClient {
    const isSingletonEnabled = options?.isSingletonEnabled ?? true; // default to true

    if (isSingletonEnabled) {
        console.log("Using singleton KafkaClient instance");
        return KafkaClient.getInstance(kafkaConfig);
    } else {
        return new KafkaClient(kafkaConfig);
    }
}

// Re-export useful types and utilities
export * from "./interface/kafka.interface";
export { KafkaConfigManager } from "./config/kafka-config";
