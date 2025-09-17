/**
 * @fileoverview Kafka connection management with singleton pattern
 * @description Manages all Kafka connections including producers, consumers, and admin clients
 */

import { Kafka, Producer, Consumer, Admin } from "kafkajs";
import { Logger } from "../logger/logger";
import { IKafkaConsumerOptions, KafkaConfig } from "../interface/kafka.interface";

/**
 * Manages Kafka connections and provides factory methods for producers, consumers, and admin clients.
 * 
 * @description This class implements the singleton pattern to ensure consistent connection
 * management across the application. It handles:
 * - Kafka client initialization
 * - Producer creation and management
 * - Consumer creation and management
 * - Admin client management
 * - Graceful shutdown of all connections
 * 
 * @example
 * ```typescript
 * const connectionManager = KafkaConnectionManager.getInstance(config);
 * const producer = await connectionManager.createProducer('my-producer');
 * const consumer = await connectionManager.createConsumer('my-group');
 * ```
 */
export class KafkaConnectionManager {
    private static instance: KafkaConnectionManager; // Singleton instance

    private kafka: Kafka;
    private producers = new Map<string, Producer>();
    private consumers = new Map<string, Consumer>();
    private admin: Admin | null = null;
    private logger: Logger;

    /**
     * Creates a new KafkaConnectionManager instance.
     * 
     * @param config - Kafka configuration
     * @private
     */
    constructor(private config: KafkaConfig) {
        this.kafka = new Kafka({
            clientId: this.config.clientId,
            brokers: this.config.brokers,
        });
        this.logger = Logger.getInstance();
        this.logger.info("Kafka connection initialized");
    }

    /**
     * Gets or creates the singleton instance of KafkaConnectionManager.
     * 
     * @param config - Kafka configuration
     * @returns The singleton KafkaConnectionManager instance
     */
    public static getInstance(config: KafkaConfig): KafkaConnectionManager {
        if (!KafkaConnectionManager.instance) {
            KafkaConnectionManager.instance = new KafkaConnectionManager(config);
        }
        return KafkaConnectionManager.instance;
    }

    /**
     * Gets the underlying Kafka client instance.
     * 
     * @returns The Kafka client instance
     */
    public getKafka(): Kafka {
        return this.kafka;
    }

    /**
     * Creates or retrieves a Kafka producer.
     * 
     * @param producerId - Unique identifier for the producer
     * @returns Promise resolving to Producer instance or undefined if creation fails
     * 
     * @description Creates a new producer if one doesn't exist with the given ID,
     * otherwise returns the existing producer. Producers are cached for reuse.
     */
    async createProducer(producerId: string): Promise<Producer | undefined> {
        try {
            if (!this.producers.has(producerId)) {
                const producer = this.kafka.producer({
                    allowAutoTopicCreation: false,
                });
                await producer.connect();
                this.producers.set(producerId, producer);
            }
            return this.producers.get(producerId)!;
        } catch (error) {
            this.logger.error(`Error while creating producer!!`);
        }
    }

    /**
     * Creates or retrieves a Kafka consumer.
     * 
     * @param groupId - Consumer group ID
     * @param options - Optional consumer configuration
     * @returns Promise resolving to Consumer instance or undefined if creation fails
     * 
     * @description Creates a new consumer if one doesn't exist with the given group ID,
     * otherwise returns the existing consumer. Consumers are cached for reuse.
     */
    async createConsumer(
        groupId: string,
        options?: IKafkaConsumerOptions
    ): Promise<Consumer | undefined> {
        try {
            if (!this.consumers.has(groupId)) {
                const consumer = this.kafka.consumer({
                    groupId,
                    maxBytes: options?.maxBytes || 262144, // Default to .25MB
                    sessionTimeout: options?.sessionTimeout || 60000, // Default to 60 seconds
                    heartbeatInterval: options?.heartbeatInterval || 30000, // Default to 30 seconds
                });
                await consumer.connect();
                this.consumers.set(groupId, consumer);
            }
            return this.consumers.get(groupId)!;
        } catch (error) {
            this.logger.error(`Error while creating consumer!!`);
        }
    }

    /**
     * Gets or creates the Kafka admin client.
     * 
     * @returns Promise resolving to Admin client instance
     * 
     * @description Creates and connects the admin client if it doesn't exist,
     * otherwise returns the existing connected admin client.
     */
    async getAdmin(): Promise<Admin> {
        if (!this.admin) {
            this.admin = this.kafka.admin();
            await this.admin.connect();
        }
        return this.admin;
    }

    /**
     * Performs graceful shutdown of all Kafka connections.
     * 
     * @returns Promise that resolves when all connections are closed
     * 
     * @description Cleanly disconnects all producers, consumers, and admin clients.
     * This method should be called during application shutdown to prevent
     * resource leaks and ensure proper cleanup.
     * 
     * @example
     * ```typescript
     * process.on('SIGTERM', async () => {
     *   await connectionManager.shutdown();
     * });
     * ```
     */
    public async shutdown(): Promise<void> {
        this.logger.info("Shutting down Kafka connection...");

        try {
            // Disconnect all producers
            for (const producer of this.producers.values()) {
                await producer.disconnect();
            }
            this.producers.clear();

            // Disconnect all consumers
            for (const consumer of this.consumers.values()) {
                await consumer.disconnect();
            }
            this.consumers.clear();

            // Disconnect admin client if it exists
            if (this.admin) {
                await this.admin.disconnect();
                this.admin = null;
            }

            this.logger.info("Kafka connection shutdown completed.");
        } catch (error) {
            this.logger.error("Error during Kafka connection shutdown:", error);
        }
    }
}
