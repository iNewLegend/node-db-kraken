import {
    DynamoDBStreams as DynamoDBStreamsInternal,
    type Shard,
    type _Stream,
    type SequenceNumberRange, type GetRecordsCommandOutput, type ShardIteratorType
} from '@aws-sdk/client-dynamodb-streams';

import { DynamoDBClient } from './dynamo-db-client';
import { DebugLogger } from '../common/debug-logger';
import type { TShardInfo } from "./dynamo-db-defs";

const debug = DebugLogger.create( 'dynamodb:internal-streams' );

interface IDynamoDBGetHorizonOptions {
    getLastShardOnly?: boolean;
    skipClosedShards?: boolean;
}

export class DynamoDBStreams {
    private streams: DynamoDBStreamsInternal;

    public constructor( protected readonly client: DynamoDBClient ) {
        const config = client.getResolvedConfig();

        this.streams = new DynamoDBStreamsInternal( {
            credentials: config.credentials,
            region: config.region,
            endpoint: config.endpoint
        } );
    }

    public async getStream( tableName: string ) {
        const listStreamsResponse = await this.streams.listStreams( {
            TableName: tableName
        } );

        if ( ! listStreamsResponse.Streams?.length ) {
            return null;
        }

        debug( () => [
            'Found streams for table',
            {
                tableName,
                streams: listStreamsResponse.Streams
            }
        ] );

        return listStreamsResponse.Streams[ 0 ];
    }

    public async getHorizon(
        tableName: string,
        options: IDynamoDBGetHorizonOptions & { getLastShardOnly: true }
    ): Promise<TShardInfo | null>;
    public async getHorizon(
        tableName: string,
        options: IDynamoDBGetHorizonOptions & { getLastShardOnly: false }
    ): Promise<TShardInfo[] | null>;

    public async getHorizon(
        tableName: string,
        options: IDynamoDBGetHorizonOptions
    ): Promise<TShardInfo | TShardInfo[] | null> {
        const stream = await this.getStream( tableName );

        if ( ! stream ) {
            return null;
        }

        const describeStreamResponse = await this.streams.describeStream( {
            StreamArn: stream.StreamArn
        } );


        debug( () => [
            'Found stream',
            describeStreamResponse
        ] );

        let shards = describeStreamResponse.StreamDescription?.Shards ?? [];

        if ( ! shards.length ) {
            debug( () => [ `No shards found for stream ${ stream.StreamArn }` ] );
            return null;
        }

        if ( options.getLastShardOnly ) {
            return this.getShardIterator( shards.at( -1 )!, stream, "TRIM_HORIZON" );
        }

        if ( options.skipClosedShards ) {
            /**
             * This filter will keep only the active shards that can potentially contain records by:
             *
             * Shards without a StartingSequenceNumber
             * Shards without an EndingSequenceNumber (active shards)
             * Shards where StartingSequenceNumber and EndingSequenceNumber are different
             */
            shards = shards.filter(
                ( shard ) =>
                    ! shard.SequenceNumberRange?.StartingSequenceNumber ||
                    ! shard.SequenceNumberRange?.EndingSequenceNumber ||
                    shard.SequenceNumberRange.EndingSequenceNumber !==
                    shard.SequenceNumberRange.StartingSequenceNumber
            );
        }

        return Promise.all(
            shards.map( async ( shard ) => {
                return await this.getShardIterator( shard, stream, "TRIM_HORIZON" );
            } )
        );
    }

    public async getAfterSequenceNumber(
        tableName: string,
        sequenceNumber: string,
    ): Promise<TShardInfo[] | null> {
        const stream = await this.getStream( tableName );

        if ( ! stream ) {
            return null;
        }

        const describeStreamResponse = await this.streams.describeStream( {
            StreamArn: stream.StreamArn
        } );

        let shards = describeStreamResponse.StreamDescription?.Shards ?? [];

        if ( ! shards.length ) {
            debug( () => [ `No shards found for stream ${ stream.StreamArn }` ] );
            return null;
        }

        const results = await Promise.all(
            shards.map(
                async ( shard ) =>
                    await this.getShardIterator( shard, stream, "AFTER_SEQUENCE_NUMBER", sequenceNumber )
                        .catch( () => undefined )
            )
        );

        return results.filter( ( result ) => result !== undefined ) as TShardInfo[];
    }

    private async getShardIterator(
        shard: Shard,
        stream: _Stream,
        type: "AT_SEQUENCE_NUMBER" | "AFTER_SEQUENCE_NUMBER",
        sequenceNumber: string
    ): Promise<TShardInfo>;

    private async getShardIterator(
        shard: Shard,
        stream: _Stream,
        type: ShardIteratorType,
        sequenceNumber?: string
    ): Promise<TShardInfo> {
        const iteratorResponse = await this.streams.getShardIterator( {
            ShardId: shard.ShardId,
            ShardIteratorType: type,
            StreamArn: stream.StreamArn,
            SequenceNumber: sequenceNumber
        } );

        return {
            shard: shard,
            iterator: iteratorResponse.ShardIterator!
        };
    }

    public async* getStreamRecordsGenerator( args: {
        shardId: string;
        shardIterator: string;
        lastRecordTimestamp?: Date;
        limit?: number;
        tillSequence: SequenceNumberRange;
        lastShardIterator: string;
    } ) {
        let currentShardIterator: string | undefined = args.shardIterator;

        do {
            const data: GetRecordsCommandOutput = await this.streams.getRecords( {
                ShardIterator: currentShardIterator,
                Limit: args.limit ?? 1000
            } );

            debug( () => [
                `Getting records for shard ${ args.shardId } with iterator ${ currentShardIterator }`
            ] );

            for ( const record of data.Records ?? [] ) {
                debug( () => [
                    `Got record with event name ${ record.eventName } and sequence number ${ record.dynamodb?.SequenceNumber }`
                ] );

                if (
                    args.lastRecordTimestamp &&
                    record.dynamodb?.ApproximateCreationDateTime
                ) {
                    if ( ! record.dynamodb.ApproximateCreationDateTime ) {
                        debug( () => [
                            `Record has no dynamodb data, skipping. Record: ${ JSON.stringify(
                                record,
                                null,
                                2
                            ) }`
                        ] );
                        continue;
                    }

                    const recordTimestamp = new Date(
                        record.dynamodb.ApproximateCreationDateTime
                    );

                    if ( recordTimestamp <= args.lastRecordTimestamp ) {
                        debug( () => [
                            `Record with timestamp ${ recordTimestamp.toISOString() } is older than last record timestamp ${ args.lastRecordTimestamp!.toISOString() }, skipping`
                        ] );
                        continue;
                    }
                }

                yield record;
            }

            currentShardIterator = data.NextShardIterator;

            if ( currentShardIterator === args.lastShardIterator ) {
                throw new Error(
                    `Shard ${ args.shardId } has no more records to process`
                );
            }

            if ( ! currentShardIterator ) {
                debug( () => [
                    `Shard ${ args.shardId } has no more records to process`
                ] );
            }
        } while ( currentShardIterator );
    }

    public async getStreamRecords( args: {
        shardId: string;
        shardIterator: string;
        lastRecordTimestamp?: Date;
        limit?: number;
    } ) {
        const records: GetRecordsCommandOutput['Records'] = [];

        debug( () => [
            `Getting records for shard ${ args.shardId } with iterator ${ args.shardIterator }`
        ] );

        const data = await this.streams.getRecords( {
            ShardIterator: args.shardIterator,
            Limit: args.limit ?? 1000
        } );

        for ( const record of data.Records ?? [] ) {
            debug( () => [
                `Got record with event name ${ record.eventName } and sequence number ${ record.dynamodb?.SequenceNumber }`
            ] );

            if (
                args.lastRecordTimestamp &&
                record.dynamodb?.ApproximateCreationDateTime
            ) {
                if ( ! record.dynamodb.ApproximateCreationDateTime ) {
                    debug( () => [
                        `Record has no dynamodb data, skipping. Record: ${ JSON.stringify(
                            record,
                            null,
                            2
                        ) }`
                    ] );

                    continue;
                }

                const recordTimestamp = new Date(
                    record.dynamodb.ApproximateCreationDateTime
                );

                if ( recordTimestamp <= args.lastRecordTimestamp ) {
                    debug( () => [
                        `Record with timestamp ${ recordTimestamp.toISOString() } is older than last record timestamp ${ args.lastRecordTimestamp!.toISOString() }, skipping`
                    ] );

                    continue;
                }
            }

            records.push( record );
        }

        if ( ! data.NextShardIterator ) {
            debug( () => [
                `Shard ${ args.shardId } has no more records to process`
            ] );
        }

        return {
            nextShardIterator: data.NextShardIterator,
            records
        };
    }

    async getLastEvaluatedShard( tableName: string ) {
        try {
            const listStreamsResponse = await this.streams.listStreams( {
                TableName: tableName
            } );

            if ( ! listStreamsResponse.Streams?.length ) {
                return undefined;
            }

            let streamArn = listStreamsResponse.LastEvaluatedStreamArn;

            if ( ! streamArn ) {
                streamArn = listStreamsResponse.Streams?.[ 0 ].StreamArn;

                if ( ! streamArn ) {
                    return undefined;
                }
            }

            const { StreamDescription } = await this.streams.describeStream( {
                StreamArn: streamArn
            } );

            if ( ! StreamDescription ) {
                throw new Error( 'No stream description found' );
            }

            const latestShard = StreamDescription.Shards?.at( -1 );

            if ( ! latestShard ) {
                throw new Error( 'No shards found' );
            }

            return latestShard;
        } catch ( e ) {
            debug( () => [
                `Error getting last evaluated shard for table ${ tableName }: ${ e }`
            ] );
        }
    }

    async getLastSequenceRange( tableName: string ) {
        const latestShard = await this.getLastEvaluatedShard( tableName );

        return latestShard?.SequenceNumberRange;
    }
}
