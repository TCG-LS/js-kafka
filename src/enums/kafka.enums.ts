/**
 * @fileoverview Kafka-related enumerations
 * @description Defines constants and enums used throughout the Kafka client
 */

/**
 * Default system topics used by the Kafka client.
 * 
 * @description These topics are reserved for internal use by the js-kafka library
 * and should not be used directly by applications.
 */
export enum DefaultTopics {
    /** 
     * Topic used for dynamic topic subscription notifications.
     * When new topics are created, notifications are sent to this topic
     * to inform consumers about new topics they should subscribe to.
     */
    TOPIC_UPDATES = 'topic-updates'
}

/**
 * Types of message handlers supported by the consumer.
 * 
 * @description Defines how messages should be processed by consumers:
 * - single: Process messages one at a time
 * - batch: Process multiple messages together for better throughput
 */
export enum TopicHandlerTypes {
    /** Process messages individually for low latency */
    single = 'single',
    /** Process messages in batches for high throughput */
    batch = 'batch'
}
