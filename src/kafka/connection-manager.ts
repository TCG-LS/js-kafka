import { Kafka, Producer, Consumer, Admin } from "kafkajs";
import { Logger } from "../logger/logger";
import { IKafkaConsumerOptions, KafkaConfig } from "../interface/kafka.interface";

export class KafkaConnectionManager {
    private static instance: KafkaConnectionManager; // Singleton instance

    private kafka: Kafka;
    private producers = new Map<string, Producer>();
    private consumers = new Map<string, Consumer>();
    private admin: Admin | null = null;
    private logger: Logger;

    constructor(private config: KafkaConfig) {
        this.kafka = new Kafka({
            clientId: this.config.clientId,
            brokers: this.config.brokers,
        });
        this.logger = Logger.getInstance();
        this.logger.info("Kafka connection initialized");
    }

    // Singleton getter
    public static getInstance(config: KafkaConfig): KafkaConnectionManager {
        if (!KafkaConnectionManager.instance) {
            KafkaConnectionManager.instance = new KafkaConnectionManager(config);
        }
        return KafkaConnectionManager.instance;
    }

    public getKafka(): Kafka {
        return this.kafka;
    }

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

    async createConsumer(
        groupId: string,
        options?: IKafkaConsumerOptions
    ): Promise<Consumer | undefined> {
        try {
            if (!this.consumers.has(groupId)) {
                const consumer = this.kafka.consumer({
                    groupId,
                    maxBytes: options?.maxBytes || 1048576, // Default to 1MB
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

    async getAdmin(): Promise<Admin> {
        if (!this.admin) {
            this.admin = this.kafka.admin();
            await this.admin.connect();
        }
        return this.admin;
    }

    /**
     * Graceful shutdown of all producers, consumers, and admin client
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
