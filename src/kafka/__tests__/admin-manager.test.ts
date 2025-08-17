import { KafkaAdminManager } from '../admin-manager';
import { KafkaConnectionManager } from '../connection-manager';
import { KafkaProducerManager } from '../producer-manager';
import { KafkaConfig } from '../../interface/kafka.interface';
import { DefaultTopics } from '../../enums/kafka.enums';

// Mock dependencies
jest.mock('../connection-manager');
jest.mock('../producer-manager');
jest.mock('../../logger/logger', () => ({
    Logger: {
        getInstance: jest.fn().mockReturnValue({
            info: jest.fn(),
            warn: jest.fn(),
            error: jest.fn(),
            debug: jest.fn(),
        }),
    },
}));

const mockAdmin = {
    createTopics: jest.fn(),
    listTopics: jest.fn(),
};

const mockConnection = {
    getAdmin: jest.fn().mockResolvedValue(mockAdmin),
} as any;

const mockProducer = {
    sendMessage: jest.fn(),
} as any;

describe('KafkaAdminManager', () => {
    const mockConfig: KafkaConfig = {
        env: 'test',
        brokers: ['localhost:9092'],
        clientId: 'test-client',
        serviceName: 'test-service',
        consumerGroupId: 'test-group',
        partitions: 3,
        replicationFactor: 2,
    };

    beforeEach(() => {
        jest.clearAllMocks();
        // Reset singleton
        (KafkaAdminManager as any)._instance = null;
    });

    describe('Singleton Pattern', () => {
        it('should return the same instance when called multiple times', () => {
            const admin1 = KafkaAdminManager.getInstance(mockConnection, mockConfig);
            const admin2 = KafkaAdminManager.getInstance(mockConnection, mockConfig);

            expect(admin1).toBe(admin2);
        });
    });

    describe('setProducer', () => {
        it('should set the producer instance', () => {
            const admin = KafkaAdminManager.getInstance(mockConnection, mockConfig);
            admin.setProducer(mockProducer);

            expect((admin as any).producer).toBe(mockProducer);
        });
    });

    describe('createTopic', () => {
        let admin: KafkaAdminManager;

        beforeEach(() => {
            admin = KafkaAdminManager.getInstance(mockConnection, mockConfig);
            admin.setProducer(mockProducer);
        });

        it('should create topic with default config values', async () => {
            const topicName = 'test-topic';
            mockAdmin.createTopics.mockResolvedValueOnce(true);

            await admin.createTopic(topicName);

            expect(mockAdmin.createTopics).toHaveBeenCalledWith({
                topics: [{
                    topic: topicName,
                    numPartitions: 3,
                    replicationFactor: 2,
                }],
            });
        });

        it('should create topic with custom parameters', async () => {
            const topicName = 'test-topic';
            mockAdmin.createTopics.mockResolvedValueOnce(true);

            await admin.createTopic(topicName, 5, 3);

            expect(mockAdmin.createTopics).toHaveBeenCalledWith({
                topics: [{
                    topic: topicName,
                    numPartitions: 5,
                    replicationFactor: 3,
                }],
            });
        });

        it('should send topic update message after successful creation', async () => {
            const topicName = 'test-topic';
            mockAdmin.createTopics.mockResolvedValueOnce(true);

            await admin.createTopic(topicName);

            expect(mockProducer.sendMessage).toHaveBeenCalledWith(
                DefaultTopics.TOPIC_UPDATES,
                { value: topicName },
                ''
            );
        });

        it('should not create topic if it already exists in topic map', async () => {
            const topicName = 'test-topic';
            
            // First call should create the topic
            mockAdmin.createTopics.mockResolvedValueOnce(true);
            await admin.createTopic(topicName);

            // Second call should skip creation
            jest.clearAllMocks();
            await admin.createTopic(topicName);

            expect(mockAdmin.createTopics).not.toHaveBeenCalled();
            expect(mockProducer.sendMessage).not.toHaveBeenCalled();
        });

        it('should handle topic already exists scenario', async () => {
            const topicName = 'existing-topic';
            mockAdmin.createTopics.mockResolvedValueOnce(false);

            await admin.createTopic(topicName);

            expect(mockAdmin.createTopics).toHaveBeenCalled();
            expect(mockProducer.sendMessage).not.toHaveBeenCalled();
        });

        it('should throw error when topic creation fails', async () => {
            const topicName = 'test-topic';
            const error = new Error('Topic creation failed');
            mockAdmin.createTopics.mockRejectedValueOnce(error);

            await expect(admin.createTopic(topicName)).rejects.toThrow(error);
        });

        it('should use fallback values when config values are undefined', async () => {
            const configWithoutDefaults: KafkaConfig = {
                env: 'test',
                brokers: ['localhost:9092'],
                clientId: 'test-client',
                serviceName: 'test-service',
                consumerGroupId: 'test-group',
            };

            // Reset singleton to create new instance with different config
            (KafkaAdminManager as any)._instance = null;
            const adminWithoutDefaults = KafkaAdminManager.getInstance(
                mockConnection,
                configWithoutDefaults
            );
            adminWithoutDefaults.setProducer(mockProducer);

            const topicName = 'test-topic';
            mockAdmin.createTopics.mockResolvedValueOnce(true);

            await adminWithoutDefaults.createTopic(topicName);

            expect(mockAdmin.createTopics).toHaveBeenCalledWith({
                topics: [{
                    topic: topicName,
                    numPartitions: 1,
                    replicationFactor: 1,
                }],
            });
        });
    });

    describe('listTopics', () => {
        let admin: KafkaAdminManager;

        beforeEach(() => {
            admin = KafkaAdminManager.getInstance(mockConnection, mockConfig);
        });

        it('should return list of topics from admin client', async () => {
            const expectedTopics = ['topic1', 'topic2', 'topic3'];
            mockAdmin.listTopics.mockResolvedValueOnce(expectedTopics);

            const topics = await admin.listTopics();

            expect(topics).toEqual(expectedTopics);
            expect(mockAdmin.listTopics).toHaveBeenCalled();
        });

        it('should handle empty topic list', async () => {
            mockAdmin.listTopics.mockResolvedValueOnce([]);

            const topics = await admin.listTopics();

            expect(topics).toEqual([]);
        });
    });

    describe('populateTopicMapOnStart', () => {
        let admin: KafkaAdminManager;

        beforeEach(() => {
            admin = KafkaAdminManager.getInstance(mockConnection, mockConfig);
        });

        it('should populate topic map with existing topics', async () => {
            const existingTopics = ['topic1', 'topic2', 'topic3'];
            mockAdmin.listTopics.mockResolvedValueOnce(existingTopics);

            await admin.populateTopicMapOnStart();

            // Verify topics are in the map by trying to create them again
            jest.clearAllMocks();
            await admin.createTopic('topic1');
            await admin.createTopic('topic2');
            await admin.createTopic('topic3');

            expect(mockAdmin.createTopics).not.toHaveBeenCalled();
        });

        it('should handle empty topic list during population', async () => {
            mockAdmin.listTopics.mockResolvedValueOnce([]);

            await expect(admin.populateTopicMapOnStart()).resolves.not.toThrow();
        });

        it('should handle errors during topic listing', async () => {
            const error = new Error('Failed to list topics');
            mockAdmin.listTopics.mockRejectedValueOnce(error);

            await expect(admin.populateTopicMapOnStart()).rejects.toThrow(error);
        });
    });

    describe('Integration Tests', () => {
        let admin: KafkaAdminManager;

        beforeEach(() => {
            admin = KafkaAdminManager.getInstance(mockConnection, mockConfig);
            admin.setProducer(mockProducer);
        });

        it('should handle complete topic lifecycle', async () => {
            // Start with empty topic list
            mockAdmin.listTopics.mockResolvedValueOnce([]);
            await admin.populateTopicMapOnStart();

            // Create a new topic
            const topicName = 'lifecycle-topic';
            mockAdmin.createTopics.mockResolvedValueOnce(true);
            await admin.createTopic(topicName);

            // Verify topic creation was called
            expect(mockAdmin.createTopics).toHaveBeenCalledWith({
                topics: [{
                    topic: topicName,
                    numPartitions: 3,
                    replicationFactor: 2,
                }],
            });

            // Verify update message was sent
            expect(mockProducer.sendMessage).toHaveBeenCalledWith(
                DefaultTopics.TOPIC_UPDATES,
                { value: topicName },
                ''
            );

            // Try to create the same topic again - should be skipped
            jest.clearAllMocks();
            await admin.createTopic(topicName);
            expect(mockAdmin.createTopics).not.toHaveBeenCalled();
        });
    });
});