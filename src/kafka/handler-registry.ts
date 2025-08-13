import {
    BatchMessageHandler,
    ITopicRegistryOptions,
    SingleMessageHandler,
} from "../interface/kafka.interface";
import { Logger } from "../logger/logger";
import { KafkaConnectionManager } from "./connection-manager";
import { KafkaConsumerManager } from "./consumer-manager";

export class KafkaTopicHandlerRegistry {
    private logger: Logger;
    private kafkaConsumer: KafkaConsumerManager;
    constructor(private readonly kafkaConnection: KafkaConnectionManager) {
        this.logger = Logger.getInstance();
        this.kafkaConsumer = KafkaConsumerManager.getInstance(this.kafkaConnection);
    }

    registerSingle(
        topic: string,
        handler: SingleMessageHandler,
        options?: ITopicRegistryOptions
    ) {
        this.logger.debug(`registerd [single][topic]: ${topic}`);
        this.kafkaConsumer.setSingleTopicHandler(topic, handler, options);
    }

    registerBatch(
        topic: string,
        handler: BatchMessageHandler,
        options?: ITopicRegistryOptions
    ) {
        this.logger.debug(`registerd [batch][topic]: ${topic}`);
        this.kafkaConsumer.setBatchTopicHandler(topic, handler, options);
    }
}
