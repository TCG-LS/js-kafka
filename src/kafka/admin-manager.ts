/**
 * @fileoverview Kafka admin operations and topic management
 * @description Handles topic creation, listing, and administrative operations
 */

import { DefaultTopics } from "../enums/kafka.enums";
import { KafkaConfig } from "../interface/kafka.interface";
import { Logger } from "../logger/logger";
import { KafkaConnectionManager } from "./connection-manager";
import { KafkaProducerManager } from "./producer-manager";

/**
 * Manages Kafka administrative operations and topic lifecycle.
 * 
 * @description This class handles all administrative operations including:
 * - Topic creation with configurable partitions and replication
 * - Topic listing and discovery
 * - Topic map management for efficient lookups
 * - Integration with producer for topic update notifications
 * 
 * @example
 * ```typescript
 * const admin = KafkaAdminManager.getInstance(connection, config);
 * await admin.createTopic('user-events', 3, 2); // 3 partitions, 2 replicas
 * const topics = await admin.listTopics();
 * ```
 */
export class KafkaAdminManager {
    private topicMap: Set<string> = new Set();
    private logger: Logger;
    private _config: KafkaConfig;
    private producer!: KafkaProducerManager;
    private static _instance: KafkaAdminManager;

    /**
     * Creates a new KafkaAdminManager instance.
     * 
     * @param kafkaConnection - Kafka connection manager
     * @param config - Kafka configuration
     * @private
     */
    constructor(
        private readonly kafkaConnection: KafkaConnectionManager,
        config: KafkaConfig
    ) {
        this.logger = Logger.getInstance();
        this._config = config;
    }

    /**
     * Gets or creates the singleton instance of KafkaAdminManager.
     * 
     * @param connection - Kafka connection manager instance
     * @param config - Kafka configuration
     * @returns The singleton KafkaAdminManager instance
     */
    public static getInstance(
        connection: KafkaConnectionManager,
        config: KafkaConfig
    ): KafkaAdminManager {
        if (!KafkaAdminManager._instance) {
            KafkaAdminManager._instance = new KafkaAdminManager(connection, config);
        }
        return KafkaAdminManager._instance;
    }

    /**
     * Sets the producer manager for sending topic update notifications.
     * 
     * @param producer - Producer manager instance
     * 
     * @description This creates a circular dependency between admin and producer
     * to enable topic update notifications when new topics are created.
     */
    public setProducer(producer: KafkaProducerManager) {
        this.producer = producer;
    }

    /**
     * Creates a new Kafka topic if it doesn't already exist.
     * 
     * @param topicName - Name of the topic to create
     * @param numPartitions - Number of partitions (defaults to config value or 1)
     * @param replicationFactor - Replication factor (defaults to config value or 1)
     * @returns Promise that resolves when topic creation is complete
     * @throws Error if topic creation fails
     * 
     * @description Creates a topic with the specified configuration. If the topic
     * already exists in the topic map, the operation is skipped. Upon successful
     * creation, sends a notification to the topic-updates topic for dynamic subscription.
     * 
     * @example
     * ```typescript
     * // Create with defaults
     * await admin.createTopic('user-events');
     * 
     * // Create with custom settings
     * await admin.createTopic('high-throughput-topic', 10, 3);
     * ```
     */
    public async createTopic(
        topicName: string,
        numPartitions = this._config.partitions || 1,
        replicationFactor = this._config.replicationFactor || 1
    ) {
        if (this.topicMap.has(topicName)) {
            return;
        }

        const admin = await this.kafkaConnection.getAdmin();
        try {
            const topicCreated = await admin.createTopics({
                topics: [{ topic: topicName, numPartitions, replicationFactor }],
            });

            if (!topicCreated) {
                this.logger.warn(`Topic ${topicName} already exists`);
                this.topicMap.add(topicName);
                return;
            }

            this.logger.info(`Topic ${topicName} created successfully`);
            this.topicMap.add(topicName);

            // Wait for 2 seconds to allow Kafka brokers to propagate topic creation
            await new Promise(resolve => setTimeout(resolve, 2000));

            await this.producer.sendMessage(
                DefaultTopics.TOPIC_UPDATES,
                { value: topicName },
                ""
            );
        } catch (error: any) {
            this.logger.error(
                `Error while creating topic: ${topicName} error:: ${error.message}`
            );
            throw error;
        }
    }

    /**
     * Lists all topics in the Kafka cluster.
     * 
     * @returns Promise resolving to array of topic names
     * 
     * @description Retrieves a list of all topics currently available in the
     * Kafka cluster. This includes both user-created topics and system topics.
     */
    public async listTopics(): Promise<string[]> {
        const admin = await this.kafkaConnection.getAdmin();
        return await admin.listTopics();
    }

    /**
     * Populates the internal topic map with existing topics.
     * 
     * @returns Promise that resolves when topic map is populated
     * 
     * @description Called during initialization to build an internal cache
     * of existing topics. This prevents unnecessary topic creation attempts
     * and improves performance by avoiding duplicate API calls.
     */
    async populateTopicMapOnStart() {
        const allTopics = await this.listTopics();
        allTopics.forEach((topic) => {
            this.topicMap.add(topic);
        });
        this.logger.info(`Successfully populated topic map`);
    }
}
