import type { _Record, SequenceNumberRange } from "@aws-sdk/client-dynamodb-streams";
import { AsyncParallelGenerator } from "../common/async-parallel-generator";
import { DebugLogger } from "../common/debug-logger";
import { DynamoDBLocalCacheStrategy } from "./cache-strategies/dynamodb-local-cache.strategy";
import type { DynamoDBClient } from "./dynamo-db-client";
import type {
    ICacheStrategy,
    IScanParallelResult,
    IStagedCacheData,
    TDynamoDBTableMetrics,
    TShardInfo
} from "./dynamo-db-defs";
import { DynamoDBStreams } from "./dynamo-db-streams";


const debug = DebugLogger.create( 'dynamodb:sync-manager' );

export class DynamoDBSyncManager {
    private cacheStrategy: ICacheStrategy;
    private streams: DynamoDBStreams | undefined;

    constructor(
        private readonly client: DynamoDBClient,
    ) {
        this.cacheStrategy = DynamoDBLocalCacheStrategy.create( "t1"
        );
    }

    private async getStreams() {
        if ( ! this.streams ) {
            this.streams = new DynamoDBStreams( this.client );
        }

        return this.streams;
    }

    public async* getSchemaSample(
        tableName: string,
        metrics: TDynamoDBTableMetrics,
        maxFetchItems = 1000
    ) {
        debug( () => [
            'Getting schema sample',
            {
                tableName,
                metrics,
                maxFetchItems
            }
        ] );

        for await ( const batch of this.client.scanGenerator(
            tableName,
            maxFetchItems,
            this.client.getApproximateItemsLimit( metrics )
        ) ) {
            yield batch;
        }
    }

    public async* getRecords(
        tableName: string,
        metrics: TDynamoDBTableMetrics,
        options = {
            useCache: false
        }
    ): AsyncGenerator<IScanParallelResult> {
        const currentSequenceNumber = await (
            await this.getStreams()
        ).getLastSequenceRange( tableName );

        if ( options.useCache ) {
            const sample = await this.cacheStrategy.get( tableName );

            if (
                sample &&
                ! ( await this.hasChanges(
                    tableName,
                    sample,
                    currentSequenceNumber,
                    metrics
                ) )
            ) {
                for await ( const batch of sample.extract! ) {
                    yield batch;
                }

                return;
            }
        }

        // Use one generator for multiple consumers.
        const [ cacheConsumer, dataConsumer, manager ] = AsyncParallelGenerator(
            () => this.client.scanParallel( tableName, metrics ),
            2
        );

        const wrotePromise = this.cacheStrategy.set(
            tableName,
            {
                metadata: {
                    tableSize: metrics.tableSizeBytes,
                    itemCount: metrics.itemCount,
                    sequenceRange: currentSequenceNumber,
                    timestamp: new Date().getTime(),
                    lastEvaluatedKey: undefined
                },
                schema: {
                    partitionKey: metrics.partitionKey
                }
            },
            cacheConsumer
        );

        setTimeout( () => manager.start(), 0 );

        yield* dataConsumer;

        wrotePromise.then( () => debug( () => [ 'wrotePromise.then' ] ) );
    }

    /**
     *
     * The difference occurs because getLastSequenceRange() returns the sequence range of the latest shard, while getStreamRecords() returns actual records from a specific shard iterator position.
     *
     * Here's what each does:
     *
     * getLastSequenceRange():
     *
     * Gets the newest shard's sequence range
     * Shows the current state of the stream
     * Returns metadata about the shard boundaries
     *
     * getStreamRecords():
     *
     * Reads actual records from a specific position
     * Returns data based on the iterators position
     * Filtered by timestamp and limit
     * They serve different purposes - sequence range tells you about stream structure and potential changes, while stream records give you the actual change data from a specific point in time.
     *
     * Using both together gives you complete change tracking capabilities - sequence range for quick change detection and stream records for detailed change data.
     */
    private async hasChanges(
        tableName: string,
        sample: IStagedCacheData | null,
        currentSequenceNumber: SequenceNumberRange | undefined,
        metrics: {
            itemCount: number;
            partitionKey: string;
            tableSizeBytes: number;
        }
    ) {
        const initialTimestamp = new Date();

        // If `tableSizeBytes` and `currentSequenceNumber` are the same then you can read from cache.
        if ( sample && currentSequenceNumber ) {
            const sampleMetrics = {
                a: sample.metadata.tableSize,
                b: sample.metadata.sequenceRange
            };
            const actualMetrics = {
                a: metrics.tableSizeBytes,
                b: currentSequenceNumber
            };

            const isSame =
                JSON.stringify( sampleMetrics ) === JSON.stringify( actualMetrics );

            if ( isSame ) {
                const streams = await this.getStreams();

                let shards: TShardInfo[] | null | undefined;

                if ( sample.metadata.sequenceRange?.StartingSequenceNumber ) {
                    shards = await this.streams?.getAfterSequenceNumber(
                        tableName,
                        sample.metadata.sequenceRange.StartingSequenceNumber
                    )
                } else {
                    shards = await streams.getHorizon( tableName, {
                        skipClosedShards: true,
                        getLastShardOnly: false,
                    } );
                }

                if ( shards?.length ) {
                    const records: _Record[] = [];
                    for ( const shard of shards ) {
                        let currentShardIterator: string | undefined = shard.iterator;

                        do {
                            const result = await streams.getStreamRecords( {
                                shardId: shard.shard.ShardId!,
                                shardIterator: currentShardIterator,
                                lastRecordTimestamp: new Date(
                                    sample.metadata.timestamp
                                ),
                            } );

                            records.push( ... result.records );

                            currentShardIterator = result.nextShardIterator;
                            const tmp = initialTimestamp;
                        } while ( currentShardIterator )
                    }

                    const hasChanges = 0 !== records.length;

                    ( ! hasChanges ) &&
                    debug( () => [
                        'Cache are synced, data returned from cache',
                        {
                            metrics,
                            meta: sample
                        }
                    ] );

                    return hasChanges;
                }
            }
        }
        return true;
    }
}
