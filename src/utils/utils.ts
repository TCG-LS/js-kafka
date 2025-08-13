import { DefaultTopics } from "../enums/kafka.enums";

export class Utils {

    static transformTopic(topic: string, entityId: string): string {
        if (topic === DefaultTopics.TOPIC_UPDATES) return topic;
        const environment = process.env.ENV || 'dev';
        const serviceName = process.env.KAFKA_SERVICE_NAME;
        return `${environment}-${serviceName}-${topic}-${entityId}.${topic}`;
    }

    static unTransformTopic(topicName: string): string {
        if (topicName === DefaultTopics.TOPIC_UPDATES) return topicName;
        const topic = topicName?.split('.')[1];
        return topic || topicName;
    }
}
