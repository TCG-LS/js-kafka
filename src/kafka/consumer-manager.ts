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

    constructor(
        private readonly kafkaConnection: KafkaConnectionManager,
        config: KafkaConfig
    ) {
        this.logger = Logger.getInstance();
        this._config = config;
    }

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

    public setAdmin(admin: KafkaAdminManager) {
        this.kafkaAdmin = admin;
    }

    async initConsumer() {
        const topics = await this.kafkaAdmin.listTopics();

        for (const topic of topics) {
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

                        const params = { topic, message: messageValue, partition, offset };
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
                    });

                    resolveOffset(batch.lastOffset());
                    await heartbeat();
                },
            });
        }
    }

    getNewTopicHandler(topic: string) {
        return (
            this.singleTopicHandlerMap.get(topic) ||
            this.batchTopicHandlerMap.get(topic)
        );
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
        const consumer = await this.kafkaConnection.createConsumer(consumerGroupId);

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
                    this._config.consumerGroupId + "-single";

                const readMessageFromBeginning =
                    handler?.options?.fromBeginning || false;

                const consumerOptions: IKafkaConsumerOptions = {
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
                    this._config.consumerGroupId + "-batch";

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
     * When dynamic topic creation we also need to subscribe it dynamically
     * Kafkajs don't support dynamic subscription to a new topic as of now (https://github.com/tulios/kafkajs/issues/371)
     * So we need to start and stop the consumer for every new topic
     *
     * In our case this is fesiable as we don't have that much topic per org and
     * when new org is added it will re subscribe fewer time
     */
    async restartConsumer(topicName: string): Promise<void> {
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
                this._config.consumerGroupId + "-batch";

            const batchConsumer = await this.kafkaConnection.createConsumer(
                consumerGroupId
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
                this._config.consumerGroupId + "-single";

            const singleConsumer = await this.kafkaConnection.createConsumer(
                consumerGroupId
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

    handleTopicUpdatesEvents = async (params: KafkaRegisteryConsumerParams) => {
        this.logger.warn(`===== ${JSON.stringify(params)} =====`);
        const topic = params.message;
        this.logger.info(`Restarting consumer for new topic added ${topic}`);
        await this.restartConsumer(topic);
        this.logger.info(`Successfully restarted consumer for new topic ${topic}`);
    };
}
