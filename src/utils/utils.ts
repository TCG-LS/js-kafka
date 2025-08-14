import { DefaultTopics } from "../enums/kafka.enums";
import { KafkaConfig } from "../interface/kafka.interface";

export class Utils {
    static transformTopic(
        topic: string,
        entityId: string,
        config: KafkaConfig
    ): string {
        if (topic === DefaultTopics.TOPIC_UPDATES) return topic;

        const environment = config.env || "dev";
        const serviceName = config.serviceName;

        let topicName = `${environment}-${serviceName}`;

        if (
            entityId &&
            entityId !== null &&
            entityId !== undefined &&
            entityId !== "" &&
            entityId !== "null" &&
            entityId !== "undefined"
        ) {
            topicName += `-${entityId}`;
        }

        return `${topicName}.${topic}`;
    }

    static unTransformTopic(topicName: string): string {
        if (topicName === DefaultTopics.TOPIC_UPDATES) return topicName;
        const topic = topicName?.split(".")[1];
        return topic || topicName;
    }
}
