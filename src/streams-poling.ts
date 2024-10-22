import { DynamoDBStreams, type GetShardIteratorCommandInput } from "@aws-sdk/client-dynamodb-streams";
import * as util from "node:util";
import { diff } from "jest-diff";

const streamsClient = new DynamoDBStreams( {
    region: "fakeRegion",
    endpoint: "http://localhost:8000",
    credentials: {
        accessKeyId: 'fakeId',
        secretAccessKey: 'fakeKey',
    }
} );

const requestedTimestamp = new Date( '2024-10-22 12:09' );

const latestStreamShardIterator: Record<string, string> = {};

function getSharedIteratorForDisplay( shardIterator: string ): string {
    function dig() {
        if ( ! shardIterator ) {
            return 'NO_ITERATOR';
        }

        // Given `arn:aws:dynamodb:ddblocal:000000000000:table/1k68NA2J4gqc6rCf_S8G/stream/2024-10-22T07:34:00.318|001| [BIG TEXT]`
        // we want to return `arn:aws:dynamodb:ddblocal:000000000000:table/1k68NA2J4gqc6rCf_S8G/stream/2024-10-22T07:34:00.318|001`
        const iteratorParts = shardIterator.split( '|' );
        return iteratorParts.slice( 0, iteratorParts.length - 1 ).join( '|' );
    }

    return util.inspect( dig(), { colors: true } );
}

async function getStreamRecords( shardId: string, shardIterator: string ): Promise<void> {
    console.log(
        `Getting records for shard ${ shardId } with iterator ${ getSharedIteratorForDisplay( shardIterator ) }...`
    );

    const params = {
        ShardIterator: shardIterator,
        Limit: 200
    };

    const data = await streamsClient.getRecords( params );

    console.log( `Got ${ data.Records?.length } records` );

    for ( const record of data.Records ?? [] ) {
        console.log(
            `Got record with event name ${ record.eventName } and sequence number ${ record.eventID }`
        );

        if ( ! record.dynamodb?.ApproximateCreationDateTime ) {
            console.log(
                `Record has no dynamodb data, skipping. Record: ${ JSON.stringify( record, null, 2 ) }`
            );
            continue;
        }

        const timestamp = new Date( record.dynamodb.ApproximateCreationDateTime );

        if ( timestamp && timestamp >= requestedTimestamp ) {
            console.log( `Change since ${ requestedTimestamp.toISOString() } detected, record approximate creation time: ${ timestamp.toISOString() }, showing diff:` );
        } else {
            continue;
        }

        const difference = diff( record.dynamodb?.NewImage, record.dynamodb?.OldImage, {
            // Make it more compact
            expand: false,
            // Ignore keys that are not present in both images
            aAnnotation: 'Expected',
            bAnnotation: 'Actual',
            // Ignore keys that are not present in both images
            // aAnnotation: 'Expected',
            // bAnnotation: 'Actual',
            // Ignore keys that are not present in both images
            // aAnnotation: 'Expected',
        } );

        console.log( difference );


        // if ( record.eventName === 'INSERT' ) {
        //     console.log( 'New item added: ', record.dynamodb?.NewImage );
        // } else if ( record.eventName === 'MODIFY' ) {
        //     console.log( 'Modified item new image: ', record.dynamodb?.NewImage );
        //     console.log( 'Modified item old image: ', record.dynamodb?.OldImage );
        // } else if ( record.eventName === 'REMOVE' ) {
        //     console.log( 'Removed item: ', record.dynamodb?.OldImage );
        // }
    }

    if ( data.NextShardIterator ) {
        latestStreamShardIterator[ shardId ] = data.NextShardIterator;
    }
}

async function pollStream(): Promise<void> {
    const availableStreams = await streamsClient.listStreams();


    for ( const stream of availableStreams.Streams || [] ) {
        console.log( `Analyzing stream for table: ${ stream.TableName }, arn: ${ stream.StreamArn }` );

        const streamDescription = await streamsClient.describeStream( {
            StreamArn: stream.StreamArn,
        } );

        const shards = streamDescription.StreamDescription?.Shards || [];

        console.log( `Found ${ shards.length } shards for stream: ${ stream.StreamArn }` );

        for ( const shard of shards ) {
            const shardId = shard.ShardId!;

            let shardIterator = latestStreamShardIterator[ shardId ];

            if ( ! shardIterator ) {
                console.log( `Shard ${ shardId } has no iterator, getting one` );

                const shardIteratorParams: GetShardIteratorCommandInput = {
                    StreamArn: stream.StreamArn!,
                    ShardId: shardId,
                    ShardIteratorType: 'TRIM_HORIZON'
                };

                const streamShardIterator =
                    await streamsClient.getShardIterator( shardIteratorParams );

                if ( ! streamShardIterator.ShardIterator ) {
                    console.log( `Shard ${ shardId } has no iterator, skipping` );
                    continue;
                }

                shardIterator = streamShardIterator.ShardIterator;

                console.log(
                    `Shard ${ shardId } has iterator: ${ getSharedIteratorForDisplay( shardIterator ) }, saving it for next time` )
            }

            if ( ! shardIterator ) {
                console.log( `Shard ${ shardId } has no iterator, skipping` );
                continue;
            }

            await getStreamRecords( shardId, shardIterator );
        }
    }
}

const shouldStop = false;

( async () => {
    do {
        console.log( "------------------------------------------------------------" )
        console.log( 'Starting stream polling...' );
        console.log( "------------------------------------------------------------" )

        await pollStream();
        console.log( 'Taking a nap for 5 seconds...' );
        await new Promise( resolve => setTimeout( resolve, 5000 ) );
    } while ( ! shouldStop )
} )();
