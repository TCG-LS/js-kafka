import { Utils } from '../utils';
import { DefaultTopics } from '../../enums/kafka.enums';
import { KafkaConfig } from '../../interface/kafka.interface';

describe('Utils', () => {
    const mockConfig: KafkaConfig = {
        env: 'test',
        brokers: ['localhost:9092'],
        clientId: 'test-client',
        serviceName: 'user-service',
        consumerGroupId: 'test-group',
    };

    describe('transformTopic', () => {
        it('should return topic as-is for TOPIC_UPDATES', () => {
            const result = Utils.transformTopic(
                DefaultTopics.TOPIC_UPDATES,
                'entity123',
                mockConfig
            );

            expect(result).toBe(DefaultTopics.TOPIC_UPDATES);
        });

        it('should transform topic with environment, service name, and entity ID', () => {
            const result = Utils.transformTopic(
                'user-created',
                'org123',
                mockConfig
            );

            expect(result).toBe('test-user-service-org123.user-created');
        });

        it('should transform topic without entity ID when not provided', () => {
            const result = Utils.transformTopic(
                'user-created',
                '',
                mockConfig
            );

            expect(result).toBe('test-user-service.user-created');
        });

        it('should handle null entity ID', () => {
            const result = Utils.transformTopic(
                'user-created',
                null as any,
                mockConfig
            );

            expect(result).toBe('test-user-service.user-created');
        });

        it('should handle undefined entity ID', () => {
            const result = Utils.transformTopic(
                'user-created',
                undefined as any,
                mockConfig
            );

            expect(result).toBe('test-user-service.user-created');
        });

        it('should handle "null" string as entity ID', () => {
            const result = Utils.transformTopic(
                'user-created',
                'null',
                mockConfig
            );

            expect(result).toBe('test-user-service.user-created');
        });

        it('should handle "undefined" string as entity ID', () => {
            const result = Utils.transformTopic(
                'user-created',
                'undefined',
                mockConfig
            );

            expect(result).toBe('test-user-service.user-created');
        });

        it('should use default env when not provided in config', () => {
            const configWithoutEnv = { ...mockConfig };
            delete (configWithoutEnv as any).env;

            const result = Utils.transformTopic(
                'user-created',
                'org123',
                configWithoutEnv
            );

            expect(result).toBe('dev-user-service-org123.user-created');
        });

        it('should handle different environments', () => {
            const prodConfig = { ...mockConfig, env: 'production' };
            const result = Utils.transformTopic(
                'user-created',
                'org123',
                prodConfig
            );

            expect(result).toBe('production-user-service-org123.user-created');
        });

        it('should handle special characters in entity ID', () => {
            const result = Utils.transformTopic(
                'user-created',
                'org-123_test',
                mockConfig
            );

            expect(result).toBe('test-user-service-org-123_test.user-created');
        });

        it('should handle numeric entity ID', () => {
            const result = Utils.transformTopic(
                'user-created',
                '12345',
                mockConfig
            );

            expect(result).toBe('test-user-service-12345.user-created');
        });
    });

    describe('unTransformTopic', () => {
        it('should return TOPIC_UPDATES as-is', () => {
            const result = Utils.unTransformTopic(DefaultTopics.TOPIC_UPDATES);
            expect(result).toBe(DefaultTopics.TOPIC_UPDATES);
        });

        it('should extract topic name from transformed topic', () => {
            const transformedTopic = 'test-user-service-org123.user-created';
            const result = Utils.unTransformTopic(transformedTopic);

            expect(result).toBe('user-created');
        });

        it('should extract topic name from transformed topic without entity ID', () => {
            const transformedTopic = 'test-user-service.user-created';
            const result = Utils.unTransformTopic(transformedTopic);

            expect(result).toBe('user-created');
        });

        it('should return original topic if no dot separator found', () => {
            const topicWithoutDot = 'simple-topic';
            const result = Utils.unTransformTopic(topicWithoutDot);

            expect(result).toBe('simple-topic');
        });

        it('should handle topic with multiple dots', () => {
            const topicWithMultipleDots = 'test-service.user.created.event';
            const result = Utils.unTransformTopic(topicWithMultipleDots);

            expect(result).toBe('user.created.event');
        });

        it('should handle empty string', () => {
            const result = Utils.unTransformTopic('');
            expect(result).toBe('');
        });

        it('should handle null input', () => {
            const result = Utils.unTransformTopic(null as any);
            expect(result).toBe(null);
        });

        it('should handle undefined input', () => {
            const result = Utils.unTransformTopic(undefined as any);
            expect(result).toBe(undefined);
        });

        it('should handle topic ending with dot', () => {
            const topicEndingWithDot = 'test-service.';
            const result = Utils.unTransformTopic(topicEndingWithDot);

            expect(result).toBe('');
        });

        it('should handle topic starting with dot', () => {
            const topicStartingWithDot = '.user-created';
            const result = Utils.unTransformTopic(topicStartingWithDot);

            expect(result).toBe('user-created');
        });
    });

    describe('Integration Tests', () => {
        it('should be reversible for normal topics', () => {
            const originalTopic = 'user-created';
            const entityId = 'org123';
            
            const transformed = Utils.transformTopic(originalTopic, entityId, mockConfig);
            const unTransformed = Utils.unTransformTopic(transformed);

            expect(unTransformed).toBe(originalTopic);
        });

        it('should be reversible for topics without entity ID', () => {
            const originalTopic = 'user-created';
            
            const transformed = Utils.transformTopic(originalTopic, '', mockConfig);
            const unTransformed = Utils.unTransformTopic(transformed);

            expect(unTransformed).toBe(originalTopic);
        });

        it('should handle TOPIC_UPDATES consistently', () => {
            const transformed = Utils.transformTopic(
                DefaultTopics.TOPIC_UPDATES,
                'entity123',
                mockConfig
            );
            const unTransformed = Utils.unTransformTopic(transformed);

            expect(transformed).toBe(DefaultTopics.TOPIC_UPDATES);
            expect(unTransformed).toBe(DefaultTopics.TOPIC_UPDATES);
        });
    });
});