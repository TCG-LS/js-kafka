// index.ts (entry point)

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

    constructor(kafkaConfig: KafkaConfig) {
        this.logger = Logger.getInstance();

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
     * Singleton access
     */
    public static getInstance(kafkaConfig: KafkaConfig): KafkaClient {
        if (!KafkaClient.instance) {
            KafkaClient.instance = new KafkaClient(kafkaConfig);
        }
        return KafkaClient.instance;
    }

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
     * Graceful shutdown
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
 * Exported helper to create/get the singleton instance
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
