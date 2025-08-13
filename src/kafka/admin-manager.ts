import { DefaultTopics } from "../enums/kafka.enums";
import { Logger } from "../logger/logger";
import { KafkaConnectionManager } from "./connection-manager";
import { KafkaProducerManager } from "./producer-manager";

export class KafkaAdminManager {
    private topicMap: Set<string> = new Set();
    private logger: Logger;
    private producer!: KafkaProducerManager;
    private static _instance: KafkaAdminManager;

    constructor(private readonly kafkaConnection: KafkaConnectionManager) {
        this.logger = Logger.getInstance();
    }

    public static getInstance(
        connection: KafkaConnectionManager
    ): KafkaAdminManager {
        if (!KafkaAdminManager._instance) {
            KafkaAdminManager._instance = new KafkaAdminManager(connection);
        }
        return KafkaAdminManager._instance;
    }

    public setProducer(producer: KafkaProducerManager) {
        this.producer = producer;
    }

    public async createTopic(
        topicName: string,
        numPartitions = +process.env.KAFKA_PARTATIONS! || 1,
        replicationFactor = +process.env.KAFKA_REPLICATION_FACTOR! || 1
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

    public async listTopics(): Promise<string[]> {
        const admin = await this.kafkaConnection.getAdmin();
        return await admin.listTopics();
    }

    async populateTopicMapOnStart() {
        const allTopics = await this.listTopics();
        allTopics.forEach((topic) => {
            this.topicMap.add(topic);
        });
        this.logger.info(`Successfully populated topic map`);
    }
}
