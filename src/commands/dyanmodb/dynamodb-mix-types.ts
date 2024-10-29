import { faker } from "@faker-js/faker";
import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";

function chunkArray<T>( array: T[], size: number ): T[][] {
    return Array.from( { length: Math.ceil( array.length / size ) }, ( _, i ) =>
        array.slice( i * size, i * size + size )
    );
}

export async function dynamoDBmixTypes( dbClient: DynamoDBClient ) {
    const tables = await dbClient.list();
    const typeTestTables = tables.filter( t => t.startsWith( 'type-test-' ) );
    const chunks = chunkArray( typeTestTables, 10 );

    let unmixedCount = 0;
    let skippedCount = 0;
    let totalProcessed = 0;

    for ( const [ chunkIndex, chunk ] of chunks.entries() ) {
        console.log( `Processing chunk ${ chunkIndex + 1 }/${ chunks.length }` );

        await Promise.all( chunk.map( async table => {
            const items = await dbClient.scan( table );
            totalProcessed++;

            if ( items.length === 5 ) {
                unmixedCount++;
                const columnNames = Object.keys( items[ 0 ] ).filter( k => k !== 'id' );
                const newItems: any[] = [];

                items.forEach( item => {
                    columnNames.forEach( colName => {
                        const value = item[ colName ];

                        columnNames.forEach( targetColName => {
                            const newItem: any = {
                                id: { S: faker.string.uuid() }
                            };

                            columnNames.forEach( c => {
                                if ( c === targetColName ) {
                                    newItem[ c ] = value;
                                } else {
                                    newItem[ c ] = items[ Math.floor( Math.random() * items.length ) ][ c ];
                                }
                            } );

                            newItems.push( newItem );
                        } );
                    } );
                } );

                await dbClient.insertChunks( table, newItems, 25 );
                console.log( `Mixed ${ newItems.length } items in ${ table } (${ totalProcessed }/${ typeTestTables.length })` );
            } else {
                skippedCount++;
                console.log( `Skipping already mixed table: ${ table } (${ totalProcessed }/${ typeTestTables.length })` );
            }
        } ) );

        if ( chunkIndex < chunks.length - 1 ) {
            await new Promise( resolve => setTimeout( resolve, 1000 ) );
        }
    }

    console.log( '\nMixing Summary:' );
    console.log( '---------------' );
    console.log( `Total tables checked: ${ typeTestTables.length }` );
    console.log( `Tables mixed: ${ unmixedCount }` );
    console.log( `Tables skipped: ${ skippedCount }` );
}
