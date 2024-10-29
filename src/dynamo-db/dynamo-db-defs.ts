import type { TableDescription } from '@aws-sdk/client-dynamodb/dist-types';

export type TDynamoDBSchema = TableDescription & {
    partitionKey: string;
};

export const attributeTypes = [ 'S', 'N', 'B', 'BOOL', 'M', 'L', 'NULL', 'SS', 'NS', 'BS' ];
