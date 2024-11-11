import * as util from 'node:util';

import {
    DynamoDBClient as DynamoDBClientInternal,
    DescribeTableCommand,
    ListTablesCommand,
    ScanCommand,
    AttributeValue,
    type DynamoDBClientResolvedConfig,
    type ScanCommandInput,
    type ClientInputEndpointParameters,
    type ScanCommandOutput,
    type ListTablesInput,
    type ScanInput, type ListTablesCommandOutput
} from '@aws-sdk/client-dynamodb';

import {
    DYNAMODB_MAX_SCAN_SIZE_IN_BYTES,
    type TDynamoDBSchema,
    type TDynamoDBTableMetrics,
    type IScanParallelResult
} from './dynamo-db-defs';
import { DebugLogger } from '../common/debug-logger';

util.inspect.defaultOptions.depth = 10;

const debug = DebugLogger.create( 'dynamodb:internal-client' );

export class DynamoDbClientBase {
    protected constructor( protected client: DynamoDBClientInternal ) {
    }

    public getResolvedConfig(): DynamoDBClientResolvedConfig {
        return this.client.config;
    }

    public async getTableMetrics(
        tableName: string,
        strict = true
    ): Promise<TDynamoDBTableMetrics> {
        const tableDescription = await this.describe( tableName );

        if ( ! tableDescription ) {
            throw new Error( `Table ${ tableName } not found.` );
        }

        const {
            TableSizeBytes = 0,
            ItemCount = 0,
            partitionKey
        } = tableDescription;

        if ( strict && ( ! TableSizeBytes || ! ItemCount || ! partitionKey ) ) {
            throw new Error(
                `Table ${ tableName } does not have required metrics.`
            );
        }

        return {
            tableSizeBytes: TableSizeBytes,
            itemCount: ItemCount,
            partitionKey
        };
    }

    public getApproximateItemsLimit(
        metrics: Awaited<ReturnType<DynamoDbClientBase['getTableMetrics']>>
    ) {
        const { tableSizeBytes, itemCount } = metrics;

        // Check for potential division by zero or invalid data
        if ( itemCount === 0 || tableSizeBytes === 0 ) {
            throw new Error(
                'Invalid metrics data: itemCount and tableSizeBytes must be greater than zero.'
            );
        }

        // Calculate the average item size in bytes
        const averageItemSizeInBytes = tableSizeBytes / itemCount;

        // Approximate number of items that can be scanned within the max response size
        const approximateItemsLimit = Math.floor(
            DYNAMODB_MAX_SCAN_SIZE_IN_BYTES / averageItemSizeInBytes
        );

        return Math.max( approximateItemsLimit, 1 );
    }

    public async testConnection( timeout: number = 10000 ) {
        debug( () => [ 'Testing connection to DynamoDB...' ] );

        return new Promise( ( resolve, reject ) => {
            const timeoutId = setTimeout( () => {
                reject( new Error( 'Timeout' ) );
            }, timeout );

            const input: ListTablesInput = {
                Limit: 1
            };

            this.client.send( new ListTablesCommand( input ) ).then(
                ( result ) => {
                    clearTimeout( timeoutId );
                    if ( result.$metadata.httpStatusCode === 200 ) {
                        resolve( true );
                    } else {
                        reject( new Error( 'Connection failed' ) );
                    }
                },
                ( error ) => {
                    clearTimeout( timeoutId );
                    reject( error );
                }
            );
        } );
    }

    async list( input: ListTablesInput = {} ) {
        const allTableNames: string[] = [];
        let lastEvaluatedTableName: string | undefined = undefined;

        do {
            const command: ListTablesCommand = new ListTablesCommand( {
                ... input,
                ExclusiveStartTableName: lastEvaluatedTableName
            } );

            debug( () => [ 'Listing tables...', input ] );

            const response: ListTablesCommandOutput = await this.client.send( command );

            if ( response.TableNames ) {
                allTableNames.push( ... response.TableNames );
            }

            lastEvaluatedTableName = response.LastEvaluatedTableName;
        } while ( lastEvaluatedTableName );

        debug( () => [
            'Tables found:',
            util.inspect( allTableNames, { compact: true } )
        ] );

        return allTableNames;
    }

    public async describe( tableName: string ): Promise<TDynamoDBSchema> {
        const command = new DescribeTableCommand( { TableName: tableName } );

        debug( () => [ `Getting schema for table: ${ tableName }` ] );

        const { Table } = await this.client.send( command );

        const partitionKey =
            Table?.KeySchema?.find( ( key ) => key.KeyType === 'HASH' )
                ?.AttributeName ?? '';

        debug( () => [ `Partition key for table ${ tableName }: ${ partitionKey }` ] );

        return {
            ... Table,
            partitionKey
        };
    }

    public async* scanGenerator(
        tableName: string,
        limit: number,
        maxChunkSize = 10
    ): AsyncGenerator<Record<string, AttributeValue>[]> {
        const items: Record<string, AttributeValue>[] = [];

        let chunkCount = 0;
        let scannedCount = 0;

        let lastEvaluatedKey: Record<string, AttributeValue> | undefined =
            undefined;

        const input: ScanCommandInput = {
            TableName: tableName,
            Limit: maxChunkSize
        };

        do {
            if ( scannedCount >= limit ) {
                break;
            }

            if ( lastEvaluatedKey ) {
                input.ExclusiveStartKey = lastEvaluatedKey;
            }

            const response = await this.client.send( new ScanCommand( input ) );

            if ( response.Items?.length ) {
                yield response.Items;

                items.concat( response.Items );

                scannedCount += response.Items.length;
            }

            lastEvaluatedKey = response.LastEvaluatedKey;

            ++chunkCount;
        } while ( lastEvaluatedKey );

        debug( () => [
            `Fetching data for table: ${ tableName } with limit: ${ maxChunkSize } chunk: ${ chunkCount } total fetched ${ items.length }`
        ] );

        return items;
    }

    public scanSegment(
        tableName: string,
        options: {
            index: number;
            segment: number;
            totalSegments: number;
            exclusiveStartKey?: Record<string, AttributeValue>;
            projectionExpression?: string;
        }
    ) {
        const scanParams: ScanInput = {
            TableName: tableName,
            TotalSegments: options.totalSegments,
            Segment: options.segment,
            ProjectionExpression: options.projectionExpression,
            ExclusiveStartKey: options.exclusiveStartKey
        };

        return {
            index: options.index,
            segment: options.segment,
            promise: this.client.send( new ScanCommand( scanParams ) ),
            result: undefined as ScanCommandOutput | undefined
        };
    }

    public async* scanParallel(
        tableName: string,
        metrics: TDynamoDBTableMetrics,
        options: {
            maxParallel?: number;
            projectionExpression?: string;
        } = {}
    ): AsyncGenerator<IScanParallelResult> {
        const { maxParallel = 30, projectionExpression = undefined } = options;

        // TODO:
        //  We need figure out what we do with that
        //  since different tables may works more efficiently with different number of parallel/segments
        const totalSegments = Math.min(
            Math.floor( this.getApproximateItemsLimit( metrics ) / 2 ),
            10000,
            metrics.itemCount
        );

        let segmentIndex = 0;
        let scannedItemsCount = 0;

        // Track `LastEvaluatedKey` for each segment
        const segmentLastKeys = new Map<
            number,
            Record<string, AttributeValue>
        >();

        debug( () => [
            'scanParallel',
            {
                tableName,
                totalSegments,
                metrics,
                maxParallel
            }
        ] );

        while ( scannedItemsCount < metrics.itemCount ) {
            const remainingSegments = totalSegments - segmentIndex;

            let pendingScans = Array.from(
                { length: Math.min( maxParallel, remainingSegments ) },
                ( _, index ) => {
                    const currentSegment = segmentIndex + index;
                    return this.scanSegment( tableName, {
                        index: currentSegment,
                        segment: currentSegment,
                        projectionExpression: projectionExpression,
                        totalSegments,
                        exclusiveStartKey: segmentLastKeys.get( currentSegment )
                    } );
                }
            );

            while ( pendingScans.length > 0 ) {
                const segment = ( p: ReturnType<typeof this.scanSegment> ) => {
                    return new Promise( ( resolve, reject ) =>
                        p.promise.then(
                            ( result ) => {
                                p.result = result;
                                resolve( p );
                            },
                            ( error ) => reject( error )
                        )
                    );
                };

                const promise = ( await Promise.race(
                    pendingScans.map( ( p ) => segment( p ) )
                ) ) as ReturnType<typeof this.scanSegment>;

                const result = promise.result!;
                const completedIndex = pendingScans.indexOf( promise );
                const completedScan = pendingScans.at( completedIndex )!;

                if ( result.LastEvaluatedKey ) {
                    segmentLastKeys.set(
                        completedScan.segment,
                        result.LastEvaluatedKey
                    );
                } else {
                    segmentLastKeys.delete( completedScan.segment );
                }

                if ( result.Items?.length ) {
                    scannedItemsCount += result.ScannedCount || 0;

                    yield {
                        index: completedIndex,
                        meta: {
                            ... result.$metadata,
                            scanned: result.ScannedCount || 0,
                            count: result.Count || 0
                        },
                        segment: completedScan.segment,
                        data: result.Items
                    };
                }

                pendingScans = pendingScans.filter(
                    ( scan ) => scan !== completedScan
                );

                // Only increment segmentIndex if no LastEvaluatedKey
                if ( ! result.LastEvaluatedKey ) {
                    segmentIndex++;
                }

                debug( () => [
                    `Processed ${ segmentIndex }/${ totalSegments } segments, items count: ${ scannedItemsCount }/${ metrics.itemCount }, remaining keys: ${ segmentLastKeys.size }`
                ] );
            }

            // If we've processed all segments but haven't scanned all items,
            // reset segmentIndex to rescan segments with LastEvaluatedKey
            if (
                segmentIndex >= totalSegments &&
                scannedItemsCount < metrics.itemCount &&
                segmentLastKeys.size > 0
            ) {
                segmentIndex = 0;
                debug( () => [
                    'Restarting scan with LastEvaluatedKeys for remaining items'
                ] );
            }
        }
    }
}
