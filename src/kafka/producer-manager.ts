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

export class KafkaProducerManager {
    private static _instance: KafkaProducerManager;
    private logger: Logger;
    private _config: KafkaConfig;
    private admin!: KafkaAdminManager;

    constructor(
        private connection: KafkaConnectionManager,
        private config: KafkaConfig
    ) {
        this.logger = Logger.getInstance();
        this._config = config;
    }

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

    public setAdmin(admin: KafkaAdminManager) {
        this.admin = admin;
    }

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
