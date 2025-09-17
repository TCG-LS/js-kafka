/**
 * @fileoverview Kafka consumer management with dynamic topic subscription
 * @description Handles consumer lifecycle, message processing, and dynamic topic subscription
 */

import { DefaultTopics, TopicHandlerTypes } from "../enums/kafka.enums";
import { v4 as uuidv4 } from "uuid";
import {
    BatchMessageHandler,
    BatchTopicMapHandler,
    IKafkaConsumerOptions,
    ITopicRegistryOptions,
    KafkaConfig,
    KafkaRegisteryConsumerParams,
    SingleMessageHandler,
    SingleTopicMapHandler,
    TopicHandler,
} from "../interface/kafka.interface";
import { KafkaConnectionManager } from "./connection-manager";
import { KafkaAdminManager } from "./admin-manager";
import { Logger } from "../logger/logger";
import { Utils } from "../utils/utils";

/**
 * Manages Kafka consumers with dynamic topic subscription capabilities.
 * 
 * @description This class handles the complex logic of consumer management including:
 * - Dynamic topic subscription based on naming patterns
 * - Both single and batch message processing
 * - Consumer lifecycle management
 * - Topic handler registration and mapping
 * - Integration with topic-updates for dynamic subscription
 * 
 * The consumer manager supports automatic subscription to topics that match
 * registered patterns, enabling dynamic scaling and topic discovery.
 * 
 * @example
 * ```typescript
 * const consumer = KafkaConsumerManager.getInstance(connection, config);
 * consumer.setSingleTopicHandler('user-events', handleUserEvents);
 * consumer.setBatchTopicHandler('analytics', handleAnalytics);
 * await consumer.initConsumer();
 * ```
 */
export class KafkaConsumerManager {
    private batchTopicHandlerMap = new Map<string, BatchTopicMapHandler>();
    private singleTopicHandlerMap = new Map<string, SingleTopicMapHandler>();
    private topicHandlerMap = new Map<string, TopicHandler>();

    private singleConsumersSet = new Set<any>();
    private batchConsumersSet = new Set<any>();

    private logger: Logger;
    private _config: KafkaConfig;
    private kafkaAdmin!: KafkaAdminManager;
    private static _instance: KafkaConsumerManager;

    /**
     * Creates a new KafkaConsumerManager instance.
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
     * Gets or creates the singleton instance of KafkaConsumerManager.
     * 
     * @param connection - Kafka connection manager instance
     * @param config - Kafka configuration
     * @returns The singleton KafkaConsumerManager instance
     */
    public static getInstance(
        connection: KafkaConnectionManager,
        config: KafkaConfig
    ): KafkaConsumerManager {
        if (!KafkaConsumerManager._instance) {
            KafkaConsumerManager._instance = new KafkaConsumerManager(
                connection,
                config
            );
        }
        return KafkaConsumerManager._instance;
    }

    /**
     * Sets the admin manager for topic operations.
     * 
     * @param admin - Admin manager instance
     * 
     * @description Creates a circular dependency with the admin manager
     * to enable topic listing and management operations.
     */
    public setAdmin(admin: KafkaAdminManager) {
        this.kafkaAdmin = admin;
    }

    /**
     * Initializes all consumers and starts message processing.
     * 
     * @returns Promise that resolves when all consumers are initialized
     * 
     * @description Performs the following initialization steps:
     * 1. Lists all existing topics from the admin manager
     * 2. Sets up subscriptions for topics matching registered handlers
     * 3. Starts both single and batch consumer processing loops
     * 
     * This method should be called after all handlers are registered.
     */
    async initConsumer() {
        const topics = await this.kafkaAdmin.listTopics();

        for (const topic of topics) {
            // Skip topics that don't belong to the current environment
            if (!this.isTopicForCurrentEnvironment(topic)) {
                this.logger.debug(`Skipping topic ${topic} - not for environment ${this._config.env}`);
                continue;
            }

            await this.transformedHandler(topic);
        }

        await Promise.all([this.runSingleConsumer(), this.runBatchConsumer()]);
    }

    private async runSingleConsumer() {
        for (const consumer of this.singleConsumersSet) {
            await consumer.run({
                eachMessage: async ({
                    topic,
                    partition,
                    message,
                    heartbeat,
                }: {
                    topic: string;
                    partition: number;
                    message: { value?: Buffer | null; offset: string };
                    heartbeat: () => Promise<void>;
                }) => {
                    try {
                        const handlerMeta = this.getHandler(topic);
                        if (!handlerMeta || handlerMeta.type !== TopicHandlerTypes.single)
                            return;

                        const handler = handlerMeta.handler as (data: any) => Promise<void>;
                        const messageValue = JSON.parse(message.value?.toString() || "{}");
                        const offset = message.offset;

                        const params = { topic, message: messageValue, partition, offset, heartbeat };
                        await handler(params);

                        await heartbeat();
                    } catch (error) {
                        this.logger.error(
                            `Error processing message from topic ${topic}:`,
                            error
                        );
                    }
                },
            });
        }
    }

    private async runBatchConsumer() {
        for (const consumer of this.batchConsumersSet) {
            await consumer.run({
                eachBatch: async ({
                    batch,
                    resolveOffset,
                    heartbeat,
                }: {
                    batch: {
                        topic: string;
                        partition: number;
                        messages: Array<{ value?: Buffer | null; offset: string }>;
                        lastOffset: () => string;
                    };
                    resolveOffset: (offset: string) => void;
                    heartbeat: () => Promise<void>;
                }) => {
                    const topic = batch.topic;
                    const handlerMeta = this.getHandler(topic);

                    if (!handlerMeta || handlerMeta.type !== TopicHandlerTypes.batch)
                        return;

                    const handler = handlerMeta.handler as (data: any) => Promise<void>;
                    const messages = [];

                    for (const message of batch.messages) {
                        try {
                            const kafkaMessage = JSON.parse(
                                message.value?.toString() ?? "{}"
                            );
                            messages.push(kafkaMessage);

                            await heartbeat();
                        } catch (error) {
                            this.logger.error(
                                `Error processing message [${batch.topic} partition ${batch.partition} offset ${message.offset}]:`,
                                error
                            );
                            await heartbeat();
                        }
                    }

                    this.logger.info(`Batch processing message ${batch.topic}`);

                    await heartbeat();

                    await handler({
                        topic,
                        messages,
                        partition: batch.partition,
                        offset: batch.lastOffset(),
                        heartbeat,
                    });

                    resolveOffset(batch.lastOffset());
                    await heartbeat();
                },
            });
        }
    }

    /**
     * Retrieves a topic handler for the specified topic.
     * 
     * @param topic - Topic name to find handler for
     * @returns Handler configuration or undefined if not found
     * 
     * @description Searches both single and batch handler maps to find
     * a registered handler for the given topic name.
     */
    getNewTopicHandler(topic: string) {
        return (
            this.singleTopicHandlerMap.get(topic) ||
            this.batchTopicHandlerMap.get(topic)
        );
    }

    /**
     * Checks if a topic belongs to the current environment.
     * 
     * @param topic - Full topic name to check
     * @returns True if topic belongs to current environment, false otherwise
     * @private
     * 
     * @description Determines if a topic should be processed by this consumer
     * based on the environment prefix. Topics should start with "{env}-" to
     * be processed by consumers in that environment.
     * 
     * Special cases:
     * - System topics (like 'topic-updates') are always processed
     * - Topics starting with the current environment prefix are processed
     * - All other topics are skipped
     * 
     * @example
     * ```typescript
     * // Environment: 'dev'
     * isTopicForCurrentEnvironment('dev-user-service-123.events'); // true
     * isTopicForCurrentEnvironment('prod-user-service-123.events'); // false
     * isTopicForCurrentEnvironment('topic-updates'); // true (system topic)
     * ```
     */
    private isTopicForCurrentEnvironment(topic: string): boolean {
        // Always process system topics
        if (topic === DefaultTopics.TOPIC_UPDATES) {
            return true;
        }

        // Check if topic starts with current environment prefix
        const envPrefix = `${this._config.env}-`;
        return topic.startsWith(envPrefix);
    }

    private async transformedHandler(
        topic: string,
        isNewTopic: boolean = false
    ): Promise<void> {
        if (topic === DefaultTopics.TOPIC_UPDATES) {
            await this.topicUpdatedSubscription(topic);
            return;
        }

        if (await this.singleTopicSubscription(topic, isNewTopic)) return;
        if (await this.batchTopicSubscription(topic, isNewTopic)) return;
    }

    private async topicUpdatedSubscription(topic: string) {
        const handler = this.singleTopicHandlerMap.get(topic);
        if (!handler) return;

        const consumerGroupId =
            handler?.options?.consumerGroup || `topic-updates-${uuidv4()}-single`;

        const consumerOptions: IKafkaConsumerOptions = {
            maxBytes: handler?.options?.maxBytes,
            sessionTimeout: handler?.options?.sessionTimeout,
            heartbeatInterval: handler?.options?.heartbeatInterval,
        };

        const consumer = await this.kafkaConnection.createConsumer(consumerGroupId, consumerOptions);

        if (!consumer) {
            this.logger.warn(
                `No consumer found with above consumer group ${consumerGroupId}`
            );
            return;
        }

        this.singleConsumersSet.add(consumer);

        this.topicHandlerMap.set(topic, {
            type: TopicHandlerTypes.single,
            handler: this.singleTopicHandlerMap.get(topic)!.handler,
        });
        await consumer.subscribe({
            topic,
            fromBeginning: false,
        });
    }

    private async singleTopicSubscription(
        topic: string,
        isNewTopic: boolean = false
    ): Promise<boolean> {
        for (const [key, value] of this.singleTopicHandlerMap.entries()) {
            const topicMatchCondition = !isNewTopic
                ? topic.endsWith(`.${key}`)
                : topic === key;

            if (topicMatchCondition) {
                const kt = topic?.split(".")[1];
                const handler = this.singleTopicHandlerMap.get(kt);
                if (!handler) return false;

                const consumerGroupId =
                    handler?.options?.consumerGroup ||
                    this._config.consumerGroupId + `-single-${this._config.env}`;

                const readMessageFromBeginning =
                    handler?.options?.fromBeginning || false;

                const consumerOptions: IKafkaConsumerOptions = {
                    maxBytes: handler?.options?.maxBytes,
                    sessionTimeout: handler?.options?.sessionTimeout,
                    heartbeatInterval: handler?.options?.heartbeatInterval,
                };

                const consumer = await this.kafkaConnection.createConsumer(
                    consumerGroupId,
                    consumerOptions
                );

                if (!consumer) {
                    this.logger.warn(
                        `No consumer found with above consumer group ${consumerGroupId}`
                    );
                    return false;
                }

                this.singleConsumersSet.add(consumer);

                this.topicHandlerMap.set(topic, {
                    type: TopicHandlerTypes.single,
                    handler: value.handler,
                });
                await consumer.subscribe({
                    topic,
                    fromBeginning: readMessageFromBeginning,
                });
                return true;
            }
        }
        return false;
    }

    private async batchTopicSubscription(
        topic: string,
        isNewTopic: boolean = false
    ): Promise<boolean> {
        for (const [key, value] of this.batchTopicHandlerMap.entries()) {
            const topicMatchCondition = !isNewTopic
                ? topic.endsWith(`.${key}`)
                : topic === key;

            if (topicMatchCondition) {
                const kt = topic?.split(".")[1];
                const handler = this.batchTopicHandlerMap.get(kt);
                if (!handler) return false;

                const consumerGroupId =
                    handler?.options?.consumerGroup ||
                    this._config.consumerGroupId + `-batch-${this._config.env}`;

                const consumerOptions: IKafkaConsumerOptions = {
                    maxBytes: handler?.options?.maxBytes,
                    sessionTimeout: handler?.options?.sessionTimeout,
                    heartbeatInterval: handler?.options?.heartbeatInterval,
                };

                const readMessageFromBeginning =
                    handler?.options?.fromBeginning || false;

                const consumer = await this.kafkaConnection.createConsumer(
                    consumerGroupId,
                    consumerOptions
                );

                if (!consumer) {
                    this.logger.warn(
                        `No consumer found with above consumer group ${consumerGroupId}`
                    );
                    return false;
                }

                this.batchConsumersSet.add(consumer);

                this.topicHandlerMap.set(topic, {
                    type: TopicHandlerTypes.batch,
                    handler: value.handler,
                });
                await consumer.subscribe({
                    topic,
                    fromBeginning: readMessageFromBeginning,
                });
                return true;
            }
        }
        return false;
    }

    private getHandler(topic: string) {
        return this.topicHandlerMap.get(topic);
    }

    /**
     * Registers a handler for single message processing.
     * 
     * @param topic - Base topic name to handle
     * @param handler - Function to process individual messages
     * @param options - Optional consumer configuration
     * 
     * @description Registers a handler that will be called for each individual
     * message received on topics matching the pattern *.{topic}.
     */
    setSingleTopicHandler(
        topic: string,
        handler: SingleMessageHandler,
        options?: ITopicRegistryOptions
    ) {
        this.singleTopicHandlerMap.set(topic, {
            type: TopicHandlerTypes.single,
            handler,
            options,
        });
    }

    /**
     * Registers a handler for batch message processing.
     * 
     * @param topic - Base topic name to handle
     * @param handler - Function to process message batches
     * @param options - Optional consumer configuration
     * 
     * @description Registers a handler that will be called with batches of
     * messages received on topics matching the pattern *.{topic}.
     */
    setBatchTopicHandler(
        topic: string,
        handler: BatchMessageHandler,
        options?: ITopicRegistryOptions
    ) {
        this.batchTopicHandlerMap.set(topic, {
            type: TopicHandlerTypes.batch,
            handler,
            options,
        });
    }

    /**
     * Restarts consumer for dynamic topic subscription.
     * 
     * @param topicName - Full topic name to subscribe to
     * @returns Promise that resolves when consumer is restarted
     * 
     * @description Due to KafkaJS limitations with dynamic topic subscription,
     * this method creates a new consumer instance for newly discovered topics.
     * It handles both single and batch consumers based on the registered handler type.
     * 
     * @see {@link https://github.com/tulios/kafkajs/issues/371} KafkaJS dynamic subscription issue
     * 
     * @example
     * ```typescript
     * // Called automatically when topic-updates notifications are received
     * await consumer.restartConsumer('prod-user-service-user123.user-events');
     * ```
     */
    async restartConsumer(topicName: string): Promise<void> {
        // Skip topics that don't belong to the current environment
        if (!this.isTopicForCurrentEnvironment(topicName)) {
            this.logger.debug(`Skipping restart for topic ${topicName} - not for environment ${this._config.env}`);
            return;
        }

        this.logger.info(`Subscribed to topic: ${topicName}`);

        const kt = Utils.unTransformTopic(topicName);

        const topicHandler = this.getNewTopicHandler(kt);

        if (!topicHandler) {
            this.logger.error(`No handler found for topic: ${topicName}`);
            return;
        }

        if (topicHandler.type === TopicHandlerTypes.batch) {
            const handler = topicHandler.handler as (data: any) => Promise<void>;

            const consumerGroupId =
                topicHandler?.options?.consumerGroup ||
                this._config.consumerGroupId + `-batch-${this._config.env}`;

            const consumerOptions: IKafkaConsumerOptions = {
                maxBytes: topicHandler?.options?.maxBytes,
                sessionTimeout: topicHandler?.options?.sessionTimeout,
                heartbeatInterval: topicHandler?.options?.heartbeatInterval,
            };

            const batchConsumer = await this.kafkaConnection.createConsumer(
                consumerGroupId,
                consumerOptions
            );

            if (!batchConsumer) {
                this.logger.warn(
                    `No consumer found with above consumer group ${consumerGroupId}`
                );
                return;
            }

            await batchConsumer.stop();
            await batchConsumer.subscribe({ topic: topicName, fromBeginning: true });

            await batchConsumer.run({
                eachBatchAutoResolve: true,
                eachBatch: async ({ batch, resolveOffset, heartbeat }) => {
                    const messages = [];
                    for (const message of batch.messages) {
                        try {
                            const kafkaMessage = JSON.parse(
                                message.value?.toString() ?? "{}"
                            );
                            messages.push(kafkaMessage);

                            await heartbeat();
                        } catch (error) {
                            this.logger.error(
                                `Error processing message [${batch.topic} partition ${batch.partition} offset ${message.offset}]:`,
                                error
                            );
                        }
                    }

                    const data = {
                        topic: batch.topic,
                        messages: messages,
                        partition: batch.partition,
                        offset: batch.lastOffset(),
                        heartbeat,
                    };
                    await handler(data);

                    resolveOffset(batch.lastOffset());
                    await heartbeat();
                },
            });
        } else if (topicHandler.type === TopicHandlerTypes.single) {
            const handler = topicHandler.handler as (data: any) => Promise<void>;

            const consumerGroupId =
                topicHandler?.options?.consumerGroup ||
                this._config.consumerGroupId + `-single-${this._config.env}`;

            const consumerOptions: IKafkaConsumerOptions = {
                maxBytes: topicHandler?.options?.maxBytes,
                sessionTimeout: topicHandler?.options?.sessionTimeout,
                heartbeatInterval: topicHandler?.options?.heartbeatInterval,
            };

            const singleConsumer = await this.kafkaConnection.createConsumer(
                consumerGroupId,
                consumerOptions
            );

            if (!singleConsumer) {
                this.logger.warn(
                    `No consumer found with above consumer group ${consumerGroupId}`
                );
                return;
            }

            await singleConsumer.stop();
            await singleConsumer.subscribe({ topic: topicName, fromBeginning: true });

            await singleConsumer.run({
                eachMessage: async ({ topic, partition, message, heartbeat }) => {
                    try {
                        const messageValue = JSON.parse(message.value?.toString() || "{}");
                        const offset = message.offset;

                        const data = {
                            topic,
                            message: messageValue,
                            partition,
                            offset,
                            heartbeat,
                        };

                        await handler(data);
                        await heartbeat();
                    } catch (error) {
                        this.logger.error(
                            `Error processing message from topic ${topic}:`,
                            error
                        );
                    }
                    await heartbeat();
                },
            });
        } else {
            this.logger.error(`Unknown handler type for topic: ${topicName}`);
        }
    }

    /**
     * Handles topic update notifications for dynamic subscription.
     * 
     * @param params - Topic update event parameters
     * @returns Promise that resolves when topic update is processed
     * 
     * @description This method is called when notifications are received on the
     * topic-updates topic. It triggers consumer restart for the new topic to
     * enable dynamic subscription without application restart.
     * 
     * @example
     * ```typescript
     * // Automatically called when topic-updates receives a notification
     * // params.message contains the new topic name
     * ```
     */
    handleTopicUpdatesEvents = async (params: KafkaRegisteryConsumerParams) => {
        this.logger.warn(`===== ${JSON.stringify(params)} =====`);
        const topic = params.message;
        this.logger.info(`Restarting consumer for new topic added ${topic}`);
        await this.restartConsumer(topic);
        this.logger.info(`Successfully restarted consumer for new topic ${topic}`);
    };
}
