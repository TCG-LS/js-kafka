import { IHeaders, KafkaMessage } from 'kafkajs';

export interface KafkaConfig {
    env?: string;
    brokers: string[];
    clientId: string;
    serviceName: string;
    partitions?: number;
    replicationFactor?: number;
    acks?: number;
    connectionTimeout?: number;
}

export interface MessagePayload {
    key?: string;
    value: any;
    timestamp?: string;
    partition?: number;
    headers?: IHeaders;
}


export type SingleMessageHandler = (
    param: ISingleKafkaConsumerParams,
) => Promise<void>;
export type BatchMessageHandler = (
    params: IBulkKafkaConsumerParam,
) => Promise<void>;

interface ISingleKafkaConsumerParams {
    topic: string;
    message: KafkaMessage;
    partition?: number;
    offset?: string;
}

interface IBulkKafkaConsumerParam {
    topic: string;
    messages: KafkaMessage[];
}

export interface ITopicRegistryOptions {
    consumerGroup?: string;
    fromBeginning?: boolean;
    maxBytes?: number;
    sessionTimeout?: number; // milliseconds
    heartbeatInterval?: number; // milliseconds
}

export interface TopicHandler {
    type: 'single' | 'batch';
    handler: SingleMessageHandler | BatchMessageHandler;
}

export interface BatchTopicMapHandler {
    type: 'batch';
    handler: BatchMessageHandler;
    options?: ITopicRegistryOptions;
}

export interface SingleTopicMapHandler {
    type: 'single';
    handler: SingleMessageHandler;
    options?: ITopicRegistryOptions;
}

export interface KafkaRegisteryConsumerParams {
    topic: string;
    message: Record<string, any>;
    partition?: number;
    offset?: string;
}

export interface IKafkaMessage {
    key?: string
    value: string
    timestamp?: string
    attributes?: number
    offset?: string
    headers?: IHeaders
    size?: never
}

export interface IKafkaConsumerOptions {
    maxBytes?: number;
    sessionTimeout?: number; // milliseconds
    heartbeatInterval?: number; // milliseconds
}

export interface IKafkaProducerOptions {
    acks?: number;
}
