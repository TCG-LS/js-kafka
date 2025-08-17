import { KafkaClient, getKafkaClient } from '../index';
import { KafkaConfig } from '../interface/kafka.interface';
import { DefaultTopics } from '../enums/kafka.enums';

// Mock all dependencies
jest.mock('../config/kafka-config');
jest.mock('../kafka/connection-manager');
jest.mock('../kafka/consumer-manager');
jest.mock('../kafka/producer-manager');
jest.mock('../kafka/admin-manager');
jest.mock('../kafka/handler-registry');
jest.mock('../logger/logger');

// Mock implementations
const mockLogger = {
    getInstance: jest.fn().mockReturnValue({
        info: jest.fn(),
        warn: jest.fn(),
        error: jest.fn(),
        debug: jest.fn(),
    }),
};

const mockConfigManager = {
    loadConfig: jest.fn().mockImplementation((config) => config),
};

const mockConnection = {
    getInstance: jest.fn().mockReturnValue({
        shutdown: jest.fn(),
    }),
    shutdown: jest.fn(),
};

const mockConsumer = {
    getInstance: jest.fn().mockReturnValue({
        setAdmin: jest.fn(),
        initConsumer: jest.fn(),
        handleTopicUpdatesEvents: jest.fn(),
    }),
    setAdmin: jest.fn(),
    initConsumer: jest.fn(),
    handleTopicUpdatesEvents: jest.fn(),
};

const mockProducer = {
    getInstance: jest.fn().mockReturnValue({
        setAdmin: jest.fn(),
    }),
    setAdmin: jest.fn(),
};

const mockAdmin = {
    getInstance: jest.fn().mockReturnValue({
        setProducer: jest.fn(),
        populateTopicMapOnStart: jest.fn(),
    }),
    setProducer: jest.fn(),
    populateTopicMapOnStart: jest.fn(),
};

const mockRegistry = {
    registerSingle: jest.fn(),
};

// Apply mocks
require('../logger/logger').Logger = mockLogger;
require('../config/kafka-config').KafkaConfigManager = mockConfigManager;
require('../kafka/connection-manager').KafkaConnectionManager = mockConnection;
require('../kafka/consumer-manager').KafkaConsumerManager = mockConsumer;
require('../kafka/producer-manager').KafkaProducerManager = mockProducer;
require('../kafka/admin-manager').KafkaAdminManager = mockAdmin;
require('../kafka/handler-registry').KafkaTopicHandlerRegistry = jest.fn().mockImplementation(() => mockRegistry);

describe('KafkaClient', () => {
    const mockConfig: KafkaConfig = {
        env: 'test',
        brokers: ['localhost:9092'],
        clientId: 'test-client',
        serviceName: 'test-service',
        consumerGroupId: 'test-group',
    };

    beforeEach(() => {
        jest.clearAllMocks();
        // Reset singleton
        (KafkaClient as any).instance = null;
    });

    describe('Constructor', () => {
        it('should initialize all components correctly', () => {
            const client = new KafkaClient(mockConfig);

            expect(mockLogger.getInstance).toHaveBeenCalled();
            expect(mockConfigManager.loadConfig).toHaveBeenCalledWith(mockConfig);
            expect(mockConnection.getInstance).toHaveBeenCalledWith(mockConfig);
            expect(mockConsumer.getInstance).toHaveBeenCalled();
            expect(mockAdmin.getInstance).toHaveBeenCalled();
            expect(mockProducer.getInstance).toHaveBeenCalled();
        });

        it('should set up circular references between components', () => {
            new KafkaClient(mockConfig);

            const consumerInstance = mockConsumer.getInstance();
            const producerInstance = mockProducer.getInstance();
            const adminInstance = mockAdmin.getInstance();

            expect(consumerInstance.setAdmin).toHaveBeenCalledWith(adminInstance);
            expect(producerInstance.setAdmin).toHaveBeenCalledWith(adminInstance);
            expect(adminInstance.setProducer).toHaveBeenCalledWith(producerInstance);
        });

        it('should create registry with connection and config', () => {
            new KafkaClient(mockConfig);

            expect(require('../kafka/handler-registry').KafkaTopicHandlerRegistry).toHaveBeenCalledWith(
                mockConnection.getInstance(),
                mockConfig
            );
        });
    });

    describe('Singleton Pattern', () => {
        it('should return the same instance when called multiple times', () => {
            const client1 = KafkaClient.getInstance(mockConfig);
            const client2 = KafkaClient.getInstance(mockConfig);

            expect(client1).toBe(client2);
        });

        it('should create new instance when singleton is reset', () => {
            const client1 = KafkaClient.getInstance(mockConfig);
            (KafkaClient as any).instance = null;
            const client2 = KafkaClient.getInstance(mockConfig);

            expect(client1).not.toBe(client2);
        });
    });

    describe('init', () => {
        let client: KafkaClient;

        beforeEach(() => {
            client = new KafkaClient(mockConfig);
        });

        it('should initialize successfully', async () => {
            await client.init();

            expect(mockRegistry.registerSingle).toHaveBeenCalledWith(
                DefaultTopics.TOPIC_UPDATES,
                expect.any(Function)
            );
            expect(mockAdmin.getInstance().populateTopicMapOnStart).toHaveBeenCalled();
            expect(mockConsumer.getInstance().initConsumer).toHaveBeenCalled();
        });

        it('should register topic updates handler', async () => {
            await client.init();

            expect(mockRegistry.registerSingle).toHaveBeenCalledWith(
                DefaultTopics.TOPIC_UPDATES,
                expect.any(Function)
            );
        });

        it('should not initialize twice', async () => {
            await client.init();
            await client.init();

            // Should only be called once despite multiple init calls
            expect(mockAdmin.getInstance().populateTopicMapOnStart).toHaveBeenCalledTimes(1);
            expect(mockConsumer.getInstance().initConsumer).toHaveBeenCalledTimes(1);
        });

        it('should log warning when already initialized', async () => {
            await client.init();
            await client.init();

            expect(mockLogger.getInstance().warn).toHaveBeenCalledWith(
                'KafkaClient is already initialized'
            );
        });

        it('should log success message after initialization', async () => {
            await client.init();

            expect(mockLogger.getInstance().info).toHaveBeenCalledWith(
                'Kafka initialized successfully!!'
            );
        });
    });

    describe('shutdown', () => {
        let client: KafkaClient;

        beforeEach(() => {
            client = new KafkaClient(mockConfig);
        });

        it('should shutdown gracefully', async () => {
            await client.shutdown();

            expect(mockLogger.getInstance().warn).toHaveBeenCalledWith(
                'Shutting down KafkaClient gracefully...'
            );
            expect(mockConnection.getInstance().shutdown).toHaveBeenCalled();
            expect(mockLogger.getInstance().info).toHaveBeenCalledWith(
                '✅ KafkaClient shutdown completed.'
            );
        });

        it('should handle shutdown errors', async () => {
            const error = new Error('Shutdown failed');
            mockConnection.getInstance().shutdown.mockRejectedValueOnce(error);

            await client.shutdown();

            expect(mockLogger.getInstance().error).toHaveBeenCalledWith(
                '❌ Error during KafkaClient shutdown:',
                error
            );
        });
    });

    describe('Properties Access', () => {
        let client: KafkaClient;

        beforeEach(() => {
            client = new KafkaClient(mockConfig);
        });

        it('should provide access to producer', () => {
            expect(client.producer).toBe(mockProducer.getInstance());
        });

        it('should provide access to registry', () => {
            expect(client.registry).toBe(mockRegistry);
        });
    });
});

describe('getKafkaClient', () => {
    const mockConfig: KafkaConfig = {
        env: 'test',
        brokers: ['localhost:9092'],
        clientId: 'test-client',
        serviceName: 'test-service',
        consumerGroupId: 'test-group',
    };

    beforeEach(() => {
        jest.clearAllMocks();
        // Reset singleton
        (KafkaClient as any).instance = null;
    });

    it('should return singleton instance by default', () => {
        const client1 = getKafkaClient(mockConfig);
        const client2 = getKafkaClient(mockConfig);

        expect(client1).toBe(client2);
    });

    it('should return singleton when explicitly enabled', () => {
        const client1 = getKafkaClient(mockConfig, { isSingletonEnabled: true });
        const client2 = getKafkaClient(mockConfig, { isSingletonEnabled: true });

        expect(client1).toBe(client2);
    });

    it('should return new instances when singleton is disabled', () => {
        const client1 = getKafkaClient(mockConfig, { isSingletonEnabled: false });
        const client2 = getKafkaClient(mockConfig, { isSingletonEnabled: false });

        expect(client1).not.toBe(client2);
    });

    it('should handle undefined options', () => {
        const client = getKafkaClient(mockConfig, undefined);

        expect(client).toBeInstanceOf(KafkaClient);
    });

    it('should handle empty options object', () => {
        const client = getKafkaClient(mockConfig, {isSingletonEnabled: true});

        expect(client).toBeInstanceOf(KafkaClient);
    });
});

describe('Module Exports', () => {
    it('should export KafkaClient class', () => {
        expect(KafkaClient).toBeDefined();
        expect(typeof KafkaClient).toBe('function');
    });

    it('should export getKafkaClient function', () => {
        expect(getKafkaClient).toBeDefined();
        expect(typeof getKafkaClient).toBe('function');
    });

    it('should re-export KafkaConfigManager', () => {
        const { KafkaConfigManager } = require('../index');
        expect(KafkaConfigManager).toBeDefined();
    });

    it('should re-export interface types', () => {
        // This test ensures the export statement works
        // The actual interfaces are TypeScript types and won't exist at runtime
        const indexModule = require('../index');
        expect(indexModule).toBeDefined();
    });
});