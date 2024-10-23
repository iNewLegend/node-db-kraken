import type { TableDescription } from '@aws-sdk/client-dynamodb/dist-types';

export type TDynamoDBSchema = TableDescription & {
    partitionKey: string;
};
