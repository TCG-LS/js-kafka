import { KafkaConfig } from "../interface/kafka.interface";

export class KafkaConfigManager {
    private static validateRequiredEnvs(): void {
        const required = [
            "KAFKA_BROKERS",
            "KAFKA_CLIENT_ID",
            "KAFKA_SERVICE_NAME",
        ];

        const missing = required.filter((key) => !process.env[key]);
        if (missing.length > 0) {
            throw new Error(
                `Missing required environment variables: ${missing.join(", ")}`
            );
        }
    }

    static loadConfig(overrides: Partial<KafkaConfig> = {}): KafkaConfig {
        this.validateRequiredEnvs();

        return {
            env: process.env.ENV || "dev",
            brokers: process.env.KAFKA_BROKERS!.split(","),
            clientId: process.env.KAFKA_CLIENT_ID!,
            serviceName: process.env.KAFKA_SERVICE_NAME!,
            partitions: parseInt(process.env.KAFKA_PARTATIONS || "3"),
            replicationFactor: parseInt(process.env.KAFKA_REPLICATION_FACTOR || "1"),
            acks: parseInt(process.env.KAFKA_MESSAGES_ACK || "1"),
            connectionTimeout: 3000,
            ...overrides,
        };
    }
}
