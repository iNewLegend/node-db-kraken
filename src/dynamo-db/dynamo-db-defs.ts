import type { TableDescription, AttributeValue } from '@aws-sdk/client-dynamodb';

import type { SequenceNumberRange, Shard } from '@aws-sdk/client-dynamodb-streams';
import type { ResponseMetadata } from '@smithy/types/dist-types/response';


/**
 * TODO: Probably best solution would be using `node:worker_threads` to parallelize scan requests.
 *
 * Scan request is limited to 1MB of data.
 * @see https://docs.aws.amazon.com/pdfs/amazondynamodb/latest/developerguide/dynamodb-dg.pdf#Scanning%20tables%20in%20DynamoDB
 *
 * workers, if needed at all depends or workers count, is depended on the size of the table.
 */
export const DYNAMODB_MAX_SCAN_SIZE_IN_BYTES = 0.9 * 1024 * 1024;

const DYNAMODB_SYMBOL_S: unique symbol = Symbol.for( 'S' ),
    DYNAMODB_SYMBOL_N: unique symbol = Symbol.for( 'N' ),
    DYNAMODB_SYMBOL_B: unique symbol = Symbol.for( 'B' ),
    DYNAMODB_SYMBOL_BOOL: unique symbol = Symbol.for( 'BOOL' ),
    DYNAMODB_SYMBOL_NULL: unique symbol = Symbol.for( 'NULL' ),
    DYNAMODB_SYMBOL_M: unique symbol = Symbol.for( 'M' ),
    DYNAMODB_SYMBOL_L: unique symbol = Symbol.for( 'L' ),
    DYNAMODB_SYMBOL_SS: unique symbol = Symbol.for( 'SS' ),
    DYNAMODB_SYMBOL_NS: unique symbol = Symbol.for( 'NS' ),
    DYNAMODB_SYMBOL_BS: unique symbol = Symbol.for( 'BS' );

export const DYNAMODB_SYMBOLS = {
    S: DYNAMODB_SYMBOL_S,
    N: DYNAMODB_SYMBOL_N,
    B: DYNAMODB_SYMBOL_B,
    BOOL: DYNAMODB_SYMBOL_BOOL,
    NULL: DYNAMODB_SYMBOL_NULL,
    M: DYNAMODB_SYMBOL_M,
    L: DYNAMODB_SYMBOL_L,
    SS: DYNAMODB_SYMBOL_SS,
    NS: DYNAMODB_SYMBOL_NS,
    BS: DYNAMODB_SYMBOL_BS
} as const;

export const DYNAMODB_STRING_TO_SYMBOLS = {
    S: DYNAMODB_SYMBOL_S,
    N: DYNAMODB_SYMBOL_N,
    B: DYNAMODB_SYMBOL_B,
    BOOL: DYNAMODB_SYMBOL_BOOL,
    NULL: DYNAMODB_SYMBOL_NULL,
    M: DYNAMODB_SYMBOL_M,
    L: DYNAMODB_SYMBOL_L,
    SS: DYNAMODB_SYMBOL_SS,
    NS: DYNAMODB_SYMBOL_NS,
    BS: DYNAMODB_SYMBOL_BS
} as const;

export enum EDynamodbRateLimiterApi {
    DEFAULT = 'default'
}

export interface ICacheStrategy {
    get( storage: string ): Promise<any>;

    set(
        storage: string,
        value: IStagedCacheData,
        generator: AsyncGenerator<IScanParallelResult>
    ): Promise<void>;

    clear( storage: string ): Promise<void>;
}

export interface IScanParallelMeta extends ResponseMetadata {
    scanned: number;
    count: number;
}

export interface IScanParallelResult {
    index: number;
    meta: IScanParallelMeta;
    segment: number;
    data: Record<string, AttributeValue>[] | undefined;
}

export interface IStagedCacheData {
    extract?: AsyncGenerator<IScanParallelResult>;
    metadata: {
        tableSize: number;
        itemCount: number;
        lastEvaluatedKey: Record<string, AttributeValue> | undefined;
        sequenceRange?: SequenceNumberRange;
        timestamp: number;
    };
    schema: {
        partitionKey: string;
        sortKey?: string;
    };
}

export type TDynamoDBDataTypeSymbolKey = keyof typeof DYNAMODB_SYMBOLS;

export type TDynamoDBDataTypeSymbol =
    ( typeof DYNAMODB_SYMBOLS )[TDynamoDBDataTypeSymbolKey];

export type TDynamoDBAttributeType = keyof AttributeValue;

export type TDynamoDBAttributeTypes = {
    [key in TDynamoDBDataTypeSymbolKey]?:
    | Set<TDynamoDBDataTypeSymbol>
    | undefined;
};

export type TDynamoDBSchema = TableDescription & {
    partitionKey: string;
};

export type TDynamoDBTableMetrics = {
    tableSizeBytes: number;
    itemCount: number;
    partitionKey: string;
};

export type TShardInfo = {
    shard: Shard;
    iterator: string;
};
