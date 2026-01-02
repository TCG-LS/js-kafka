import { DefaultTopics, TopicHandlerTypes } from '../kafka.enums';

describe('Kafka Enums', () => {
    describe('DefaultTopics', () => {
        it('should have TOPIC_UPDATES constant', () => {
            expect(DefaultTopics.TOPIC_UPDATES).toBe('topic-updates');
        });

        it('should have correct value', () => {
            expect(DefaultTopics.TOPIC_UPDATES).toBe('topic-updates');
        });
    });

    describe('TopicHandlerTypes', () => {
        it('should have single handler type', () => {
            expect(TopicHandlerTypes.single).toBe('single');
        });

        it('should have batch handler type', () => {
            expect(TopicHandlerTypes.batch).toBe('batch');
        });

        it('should have batch manual commit handler type', () => {
            expect(TopicHandlerTypes.batchManualCommit).toBe('batchManualCommit');
        });

        it('should have correct values', () => {
            expect(TopicHandlerTypes.single).toBe('single');
            expect(TopicHandlerTypes.batch).toBe('batch');
            expect(TopicHandlerTypes.batchManualCommit).toBe('batchManualCommit');
        });

        it('should have exactly three handler types', () => {
            const values = Object.values(TopicHandlerTypes);
            expect(values).toHaveLength(3);
            expect(values).toContain('single');
            expect(values).toContain('batch');
            expect(values).toContain('batchManualCommit');
        });
    });
});