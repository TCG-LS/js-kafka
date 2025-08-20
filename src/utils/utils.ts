/**
 * @fileoverview Utility functions for topic name transformation
 * @description Provides utilities for converting between user-friendly topic names
 * and the internal topic naming convention used by js-kafka
 */

import { DefaultTopics } from "../enums/kafka.enums";
import { KafkaConfig } from "../interface/kafka.interface";

/**
 * Utility class for topic name transformations and other helper functions.
 * 
 * @description Provides static methods for transforming topic names between
 * user-friendly names and the internal naming convention used by js-kafka.
 * The naming convention is: {env}-{serviceName}-{entityId}.{topicName}
 */
export class Utils {
    /**
     * Transforms a user-friendly topic name into the internal naming convention.
     * 
     * @param topic - The base topic name (e.g., 'user-events')
     * @param entityId - Entity identifier for topic partitioning (e.g., 'user123')
     * @param config - Kafka configuration containing env and serviceName
     * @returns Transformed topic name following the convention
     * 
     * @description Converts a simple topic name into the full topic name using
     * the pattern: {env}-{serviceName}-{entityId}.{topicName}
     * 
     * Special cases:
     * - TOPIC_UPDATES is returned as-is (reserved system topic)
     * - Empty/null/undefined entityId is omitted from the name
     * - String values 'null' and 'undefined' are treated as empty
     * 
     * @example
     * ```typescript
     * const config = { env: 'prod', serviceName: 'user-service' };
     * 
     * // With entity ID
     * Utils.transformTopic('user-events', 'user123', config);
     * // Returns: 'prod-user-service-user123.user-events'
     * 
     * // Without entity ID
     * Utils.transformTopic('notifications', '', config);
     * // Returns: 'prod-user-service.notifications'
     * 
     * // System topic (unchanged)
     * Utils.transformTopic('topic-updates', 'any', config);
     * // Returns: 'topic-updates'
     * ```
     */
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

    /**
     * Extracts the base topic name from a transformed topic name.
     * 
     * @param topicName - The full transformed topic name
     * @returns The base topic name
     * 
     * @description Reverses the topic transformation by extracting everything
     * after the first dot (.) in the topic name. This gives you back the
     * original topic name that was passed to transformTopic.
     * 
     * Special cases:
     * - TOPIC_UPDATES is returned as-is (reserved system topic)
     * - Topics without dots are returned unchanged
     * - Handles multiple dots correctly (takes everything after the first dot)
     * 
     * @example
     * ```typescript
     * // Standard transformed topic
     * Utils.unTransformTopic('prod-user-service-user123.user-events');
     * // Returns: 'user-events'
     * 
     * // Topic with multiple dots
     * Utils.unTransformTopic('dev-service.user.created.event');
     * // Returns: 'user.created.event'
     * 
     * // System topic (unchanged)
     * Utils.unTransformTopic('topic-updates');
     * // Returns: 'topic-updates'
     * 
     * // No dots (unchanged)
     * Utils.unTransformTopic('simple-topic');
     * // Returns: 'simple-topic'
     * ```
     */
    static unTransformTopic(topicName: string): string {
        if (topicName === DefaultTopics.TOPIC_UPDATES) return topicName;
        const dotIndex = topicName?.indexOf(".");
        if (dotIndex !== undefined && dotIndex !== -1) {
            return topicName.substring(dotIndex + 1);
        }
        return topicName;
    }
}
