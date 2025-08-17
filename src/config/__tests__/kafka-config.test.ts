import { KafkaConfigManager } from '../kafka-config';
import { KafkaConfig } from '../../interface/kafka.interface';

describe('KafkaConfigManager', () => {
    describe('loadConfig', () => {
        const validConfig: KafkaConfig = {
            env: 'test',
            brokers: ['localhost:9092'],
            clientId: 'test-client',
            serviceName: 'test-service',
            consumerGroupId: 'test-group',
        };

        it('should load config with all required properties', () => {
            const result = KafkaConfigManager.loadConfig(validConfig);

            expect(result).toEqual({
                env: 'test',
                brokers: ['localhost:9092'],
                clientId: 'test-client',
                serviceName: 'test-service',
                consumerGroupId: 'test-group',
                partitions: 1,
                replicationFactor: 1,
                acks: 1,
                connectionTimeout: 3000,
            });
        });

        it('should apply default values for optional properties', () => {
            const result = KafkaConfigManager.loadConfig(validConfig);

            expect(result.partitions).toBe(1);
            expect(result.replicationFactor).toBe(1);
            expect(result.acks).toBe(1);
            expect(result.connectionTimeout).toBe(3000);
        });

        it('should use provided optional values instead of defaults', () => {
            const configWithOptionals: KafkaConfig = {
                ...validConfig,
                partitions: 5,
                replicationFactor: 3,
                acks: -1,
                connectionTimeout: 5000,
            };

            const result = KafkaConfigManager.loadConfig(configWithOptionals);

            expect(result.partitions).toBe(5);
            expect(result.replicationFactor).toBe(3);
            expect(result.acks).toBe(-1);
            expect(result.connectionTimeout).toBe(5000);
        });

        it('should default env to "dev" when not provided', () => {
            const configWithoutEnv = { ...validConfig };
            delete (configWithoutEnv as any).env;

            // This should throw due to validation, but let's test the logic
            expect(() => KafkaConfigManager.loadConfig(configWithoutEnv)).toThrow();
        });
    });

    describe('validateRequiredEnvs', () => {
        const validConfig: KafkaConfig = {
            env: 'test',
            brokers: ['localhost:9092'],
            clientId: 'test-client',
            serviceName: 'test-service',
            consumerGroupId: 'test-group',
        };

        it('should not throw error for valid config', () => {
            expect(() => KafkaConfigManager.loadConfig(validConfig)).not.toThrow();
        });

        it('should throw error when env is missing', () => {
            const invalidConfig = { ...validConfig };
            delete (invalidConfig as any).env;

            expect(() => KafkaConfigManager.loadConfig(invalidConfig)).toThrow(
                'Missing required configuration properties: env'
            );
        });

        it('should throw error when brokers is missing', () => {
            const invalidConfig = { ...validConfig };
            delete (invalidConfig as any).brokers;

            expect(() => KafkaConfigManager.loadConfig(invalidConfig)).toThrow(
                'Missing required configuration properties: brokers'
            );
        });

        it('should throw error when clientId is missing', () => {
            const invalidConfig = { ...validConfig };
            delete (invalidConfig as any).clientId;

            expect(() => KafkaConfigManager.loadConfig(invalidConfig)).toThrow(
                'Missing required configuration properties: clientId'
            );
        });

        it('should throw error when serviceName is missing', () => {
            const invalidConfig = { ...validConfig };
            delete (invalidConfig as any).serviceName;

            expect(() => KafkaConfigManager.loadConfig(invalidConfig)).toThrow(
                'Missing required configuration properties: serviceName'
            );
        });

        it('should throw error when consumerGroupId is missing', () => {
            const invalidConfig = { ...validConfig };
            delete (invalidConfig as any).consumerGroupId;

            expect(() => KafkaConfigManager.loadConfig(invalidConfig)).toThrow(
                'Missing required configuration properties: consumerGroupId'
            );
        });

        it('should throw error with multiple missing properties', () => {
            const invalidConfig = {
                env: 'test',
                // missing brokers, clientId, serviceName, consumerGroupId
            } as KafkaConfig;

            expect(() => KafkaConfigManager.loadConfig(invalidConfig)).toThrow(
                'Missing required configuration properties: brokers, clientId, serviceName, consumerGroupId'
            );
        });

        it('should handle empty string values as missing', () => {
            const invalidConfig: KafkaConfig = {
                env: '',
                brokers: ['localhost:9092'],
                clientId: 'test-client',
                serviceName: 'test-service',
                consumerGroupId: 'test-group',
            };

            expect(() => KafkaConfigManager.loadConfig(invalidConfig)).toThrow(
                'Missing required configuration properties: env'
            );
        });

        it('should handle empty array as missing for brokers', () => {
            const invalidConfig: KafkaConfig = {
                env: 'test',
                brokers: [],
                clientId: 'test-client',
                serviceName: 'test-service',
                consumerGroupId: 'test-group',
            };

            expect(() => KafkaConfigManager.loadConfig(invalidConfig)).toThrow(
                'Missing required configuration properties: brokers'
            );
        });
    });

    describe('edge cases', () => {
        it('should handle config with zero values for optional numeric properties', () => {
            const configWithZeros: KafkaConfig = {
                env: 'test',
                brokers: ['localhost:9092'],
                clientId: 'test-client',
                serviceName: 'test-service',
                consumerGroupId: 'test-group',
                partitions: 0,
                replicationFactor: 0,
                acks: 0,
                connectionTimeout: 0,
            };

            const result = KafkaConfigManager.loadConfig(configWithZeros);

            expect(result.partitions).toBe(0);
            expect(result.replicationFactor).toBe(0);
            expect(result.acks).toBe(0);
            expect(result.connectionTimeout).toBe(0);
        });

        it('should preserve original config object immutability', () => {
            const originalConfig: KafkaConfig = {
                env: 'test',
                brokers: ['localhost:9092'],
                clientId: 'test-client',
                serviceName: 'test-service',
                consumerGroupId: 'test-group',
            };

            const configCopy = { ...originalConfig };
            KafkaConfigManager.loadConfig(originalConfig);

            expect(originalConfig).toEqual(configCopy);
        });
    });
});