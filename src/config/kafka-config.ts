import { KafkaConfig } from "../interface/kafka.interface";

export class KafkaConfigManager {
    private static validateRequiredEnvs(config: KafkaConfig): void {
        const required = [
            "env",
            "brokers",
            "clientId",
            "serviceName",
            "consumerGroupId",
        ];

        const missing = required.filter((key) => !config[key as keyof KafkaConfig]);
        if (missing.length > 0) {
            throw new Error(
                `Missing required configuration properties: ${missing.join(", ")}`
            );
        }
    }

    static loadConfig(config: KafkaConfig): KafkaConfig {
        this.validateRequiredEnvs(config);

        return {
            env: config.env || "dev",
            brokers: config.brokers,
            clientId: config.clientId,
            serviceName: config.serviceName,
            consumerGroupId: config.consumerGroupId,
            partitions: config.partitions || 1,
            replicationFactor: config.replicationFactor || 1,
            acks: config.acks || 1,
            connectionTimeout: config.connectionTimeout || 3000,
        };
    }
}
