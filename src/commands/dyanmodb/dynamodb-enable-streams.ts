import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";

function chunkArray<T>( array: T[], size: number ): T[][] {
    return Array.from( { length: Math.ceil( array.length / size ) }, ( _, i ) =>
        array.slice( i * size, i * size + size )
    );
}

export async function verifyStreams( dbClient: DynamoDBClient ) {
    const tables = await dbClient.list();

    if ( ! tables?.length ) {
        console.log( 'No tables found' );
        return;
    }

    const typeTestTables = tables.filter( t => t.startsWith( 'type-test-' ) );
    const chunks = chunkArray( typeTestTables, 10 );

    const results = [];
    let processed = 0;

    for ( const [ index, chunk ] of chunks.entries() ) {
        console.log( `Verifying chunk ${ index + 1 }/${ chunks.length }` );

        const chunkResults = await Promise.all( chunk.map( async table => {
            const description = await dbClient.describe( table );
            processed++;

            console.log( `Verified ${ table } (${ processed }/${ typeTestTables.length })` );

            return {
                tableName: table,
                streamEnabled: description.StreamSpecification?.StreamEnabled === true,
                streamViewType: description.StreamSpecification?.StreamViewType === 'NEW_AND_OLD_IMAGES',
                streamArn: description.LatestStreamArn
            };
        } ) );

        results.push( ... chunkResults );

        if ( index < chunks.length - 1 ) {
            await new Promise( resolve => setTimeout( resolve, 1000 ) );
        }
    }

    console.log( '\nStream Status Report:' );
    console.log( '-------------------' );

    const disabled = results.filter( r => ! r.streamEnabled );
    const wrongType = results.filter( r => r.streamEnabled && ! r.streamViewType );

    console.log( `Total tables: ${ results.length }` );
    console.log( `Streams enabled: ${ results.length - disabled.length }` );
    console.log( `Correct view type: ${ results.length - wrongType.length }` );

    if ( disabled.length > 0 ) {
        console.log( '\nTables without streams:', disabled.map( r => r.tableName ) );
    }

    if ( wrongType.length > 0 ) {
        console.log( '\nTables with wrong stream type:', wrongType.map( r => r.tableName ) );
    }

    return results;
}

export async function dynamoDBenableStreams( dbClient: DynamoDBClient ) {
    const tables = await dbClient.list();

    if ( ! tables?.length ) {
        console.log( 'No tables found' );
        return;
    }

    const typeTestTables = tables.filter( t => t.startsWith( 'type-test-' ) );
    const chunks = chunkArray( typeTestTables, 10 ); // Process 10 tables at a time

    let processed = 0;

    for ( const [ index, chunk ] of chunks.entries() ) {
        console.log( `Processing chunk ${ index + 1 }/${ chunks.length }` );

        await Promise.all( chunk.map( async table => {
            try {
                await dbClient.update( {
                    TableName: table,
                    StreamSpecification: {
                        StreamEnabled: true,
                        StreamViewType: "NEW_AND_OLD_IMAGES"
                    }
                } );
                processed++;
                console.log( `Enabled streams for ${ table } (${ processed }/${ typeTestTables.length })` );
            } catch ( e ) {
                if ( e instanceof Error && e.message.includes( 'Table already has an enabled' ) ) {
                    console.log( `Skipping ${ table }: ${ e.message }` );
                    return;
                }
                console.error( `Failed to enable streams for ${ table }:`, e )
            }
        } ) );

        // Add delay between chunks
        if ( index < chunks.length - 1 ) {
            await new Promise( resolve => setTimeout( resolve, 1000 ) );
        }
    }

    await verifyStreams( dbClient );
}

