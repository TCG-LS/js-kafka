/**
 * @fileoverview Configuration management for Kafka client
 * @description Handles validation and loading of Kafka configuration with defaults
 */

import { KafkaConfig } from "../interface/kafka.interface";

/**
 * Manages Kafka configuration validation and loading.
 * 
 * @description This class provides static methods to validate and load Kafka
 * configuration with appropriate defaults. It ensures all required properties
 * are present and applies sensible defaults for optional properties.
 */
export class KafkaConfigManager {
    /**
     * Validates that all required configuration properties are present.
     * 
     * @param config - The Kafka configuration to validate
     * @throws Error if any required properties are missing or invalid
     * 
     * @description Checks for the presence of required configuration properties:
     * - env: Environment name
     * - brokers: Array of broker addresses (must not be empty)
     * - clientId: Unique client identifier
     * - serviceName: Service name for topic naming
     * - consumerGroupId: Consumer group identifier
     * 
     * @private
     */
    private static validateRequiredEnvs(config: KafkaConfig): void {
        const required = [
            "env",
            "brokers",
            "clientId",
            "serviceName",
            "consumerGroupId",
        ];

        const missing = required.filter((key) => {
            const value = config[key as keyof KafkaConfig];
            return !value || (Array.isArray(value) && value.length === 0);
        });
        if (missing.length > 0) {
            throw new Error(
                `Missing required configuration properties: ${missing.join(", ")}`
            );
        }
    }

    /**
     * Loads and validates Kafka configuration with defaults.
     * 
     * @param config - The input Kafka configuration
     * @returns Validated configuration with defaults applied
     * @throws Error if validation fails
     * 
     * @description This method:
     * 1. Validates all required properties are present
     * 2. Applies default values for optional properties:
     *    - partitions: 1
     *    - replicationFactor: 1
     *    - acks: 1
     *    - connectionTimeout: 3000ms
     * 3. Returns a complete, validated configuration object
     * 
     * @example
     * ```typescript
     * const config = KafkaConfigManager.loadConfig({
     *   env: 'dev',
     *   brokers: ['localhost:9092'],
     *   clientId: 'my-service',
     *   serviceName: 'user-service',
     *   consumerGroupId: 'user-group'
     * });
     * // Returns config with defaults applied
     * ```
     */
    static loadConfig(config: KafkaConfig): KafkaConfig {
        this.validateRequiredEnvs(config);

        return {
            env: config.env || "dev",
            brokers: config.brokers,
            clientId: config.clientId,
            serviceName: config.serviceName,
            consumerGroupId: config.consumerGroupId,
            partitions: config.partitions ?? 1,
            replicationFactor: config.replicationFactor ?? 1,
            acks: config.acks ?? 1,
            connectionTimeout: config.connectionTimeout ?? 3000,
        };
    }
}
