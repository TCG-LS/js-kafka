/**
 * @fileoverview Kafka message production with automatic topic creation
 * @description Handles sending messages to Kafka topics with support for both single and batch operations
 */

import { DefaultTopics } from "../enums/kafka.enums";
import {
    IKafkaProducerOptions,
    KafkaConfig,
    MessagePayload,
} from "../interface/kafka.interface";
import { Logger } from "../logger/logger";
import { Utils } from "../utils/utils";
import { KafkaConnectionManager } from "./connection-manager";
import { KafkaAdminManager } from "./admin-manager";

/**
 * Manages Kafka message production with automatic topic creation.
 * 
 * @description This class handles sending messages to Kafka topics with:
 * - Automatic topic creation using the naming convention
 * - Support for single and batch message sending
 * - Proper message formatting and header handling
 * - Integration with admin manager for topic management
 * - Configurable acknowledgment settings
 * 
 * @example
 * ```typescript
 * const producer = KafkaProducerManager.getInstance(connection, config);
 * await producer.sendMessage('user-events', message, 'user123');
 * await producer.sendBatchMessages('notifications', messages, 'batch1');
 * ```
 */
export class KafkaProducerManager {
    private static _instance: KafkaProducerManager;
    private logger: Logger;
    private _config: KafkaConfig;
    private admin!: KafkaAdminManager;

    /**
     * Creates a new KafkaProducerManager instance.
     * 
     * @param connection - Kafka connection manager
     * @param config - Kafka configuration
     * @private
     */
    constructor(
        private connection: KafkaConnectionManager,
        private config: KafkaConfig
    ) {
        this.logger = Logger.getInstance();
        this._config = config;
    }

    /**
     * Gets or creates the singleton instance of KafkaProducerManager.
     * 
     * @param connection - Kafka connection manager instance
     * @param config - Kafka configuration
     * @returns The singleton KafkaProducerManager instance
     */
    public static getInstance(
        connection: KafkaConnectionManager,
        config: KafkaConfig
    ): KafkaProducerManager {
        if (!KafkaProducerManager._instance) {
            KafkaProducerManager._instance = new KafkaProducerManager(
                connection,
                config
            );
        }
        return KafkaProducerManager._instance;
    }

    /**
     * Sets the admin manager for topic creation.
     * 
     * @param admin - Admin manager instance
     * 
     * @description Creates a circular dependency with the admin manager
     * to enable automatic topic creation when sending messages.
     */
    public setAdmin(admin: KafkaAdminManager) {
        this.admin = admin;
    }

    /**
     * Sends a single message to a Kafka topic.
     * 
     * @param topic - Base topic name (will be transformed using naming convention)
     * @param message - Message payload to send
     * @param entityId - Entity identifier for topic naming and partitioning
     * @param options - Optional producer configuration
     * @returns Promise that resolves when message is sent
     * @throws Error if message sending fails
     * 
     * @description Sends a message to a topic using the internal naming convention.
     * The topic will be automatically created if it doesn't exist. Message headers
     * are properly formatted for Kafka compatibility.
     * 
     * @example
     * ```typescript
     * await producer.sendMessage('user-events', {
     *   key: 'user-123',
     *   value: { action: 'login', timestamp: Date.now() },
     *   headers: { source: 'auth-service' }
     * }, 'user-123', { acks: -1 });
     * ```
     */
    async sendMessage(
        topic: string,
        message: MessagePayload,
        entityId: string = "",
        options?: IKafkaProducerOptions
    ): Promise<void> {
        const transformedTopic = Utils.transformTopic(
            topic,
            entityId,
            this._config
        );

        try {
            await this.admin.createTopic(transformedTopic);
            const producer = await this.connection.createProducer("default-producer");

            if (!producer) {
                this.logger.warn(`No producer found`);
                return;
            }

            const kafkaMessage: any = {
                key: message.key || null,
                value: JSON.stringify(message.value),
                timestamp: message?.timestamp || Date.now().toString(),
            };

            if (message?.headers) {
                // Ensure headers are in correct format for Kafka
                kafkaMessage.headers = this.formatHeaders(message.headers);
            }

            if (message?.partition !== undefined) {
                if (message.partition >= 0) {
                    kafkaMessage.partition = message.partition;
                }
            }

            await producer.send({
                topic: transformedTopic,
                messages: [kafkaMessage],
                acks: options?.acks ?? this.config.acks ?? 1,
            });

            this.logger.info(`Message sent to topic: ${transformedTopic}`);
        } catch (error) {
            this.logger.error(
                `Error sending message to topic: ${transformedTopic}`,
                error
            );
            throw error;
        }
    }

    /**
     * Sends multiple messages to a Kafka topic in a single batch.
     * 
     * @param topic - Base topic name (will be transformed using naming convention)
     * @param messages - Array of message payloads to send
     * @param entityId - Entity identifier for topic naming and partitioning
     * @param options - Optional producer configuration
     * @returns Promise that resolves when all messages are sent
     * @throws Error if batch sending fails
     * 
     * @description Sends multiple messages in a single batch operation for better
     * throughput. The topic will be automatically created if it doesn't exist.
     * All messages are formatted and sent together to improve performance.
     * 
     * @example
     * ```typescript
     * const messages = [
     *   { key: 'user-1', value: { action: 'login' } },
     *   { key: 'user-2', value: { action: 'logout' } }
     * ];
     * await producer.sendBatchMessages('user-events', messages, 'batch-1');
     * ```
     */
    async sendBatchMessages(
        topic: string,
        messages: MessagePayload[],
        entityId: string = "",
        options?: IKafkaProducerOptions
    ): Promise<void> {
        const transformedTopic = Utils.transformTopic(
            topic,
            entityId,
            this._config
        );

        try {
            await this.admin.createTopic(transformedTopic);
            const producer = await this.connection.createProducer("batch-producer");

            if (!producer) {
                this.logger.warn(`No producer found`);
                return;
            }

            const kafkaMessages = messages.map((msg) => {
                const kafkaMessage: any = {
                    key: msg.key || null,
                    value: JSON.stringify(msg.value),
                    timestamp: msg.timestamp || Date.now().toString(),
                };

                if (msg?.headers) {
                    // Ensure headers are in correct format for Kafka
                    kafkaMessage.headers = this.formatHeaders(msg.headers);
                }

                if (msg?.partition !== undefined) {
                    if (msg.partition >= 0) {
                        kafkaMessage.partition = msg.partition;
                    }
                }
                return kafkaMessage;
            });

            if (!kafkaMessages.length) {
                this.logger.warn(`No messages to send to topic: ${transformedTopic}`);
                return;
            }

            await producer.send({
                topic: transformedTopic,
                messages: kafkaMessages,
                acks: options?.acks ?? this.config.acks ?? 1,
            });

            this.logger.info(`Batch messages sent to topic: ${transformedTopic}`);
        } catch (error) {
            this.logger.error(
                `Error sending batch messages to topic: ${transformedTopic}`,
                error
            );
            throw error;
        }
    }

    /**
     * Formats message headers for Kafka compatibility.
     * 
     * @param headers - Raw headers object
     * @returns Formatted headers with string or Buffer values
     * @private
     * 
     * @description Converts header values to strings or preserves Buffers
     * to ensure compatibility with Kafka's header format requirements.
     */
    private formatHeaders(
        headers: Record<string, any>
    ): Record<string, string | Buffer> {
        const formattedHeaders: Record<string, string | Buffer> = {};

        for (const [key, value] of Object.entries(headers)) {
            if (typeof value === "string" || Buffer.isBuffer(value)) {
                formattedHeaders[key] = value;
            } else {
                // Convert other types to string
                formattedHeaders[key] = String(value);
            }
        }

        return formattedHeaders;
    }
}
