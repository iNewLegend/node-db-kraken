import { BatchWriteItemCommand } from "@aws-sdk/client-dynamodb";
import { faker } from "@faker-js/faker";
import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";

export async function dynamoDBseed( dbClient: DynamoDBClient, commandIndex: number ) {
    const tablesCount = parseInt( process.argv[ commandIndex + 1 ], 10 ) || 1;
    const itemsCount = parseInt( process.argv[ commandIndex + 2 ], 10 ) || 10;


    console.log( `Seeding ${ tablesCount } tables with ${ itemsCount } items each.` );
    const seedGenerators = await import( "../../dynamo-db/dynamo-db-seed-generator" );

    const { getAllGenerators, generateAttributeName, generateTableOrIndexName } = seedGenerators;

    const attributeNames = getAllGenerators().map( () => generateAttributeName() );
    const item: any = {};

    getAllGenerators().forEach( ( generator, index ) => {
        item[ attributeNames[ index ] ] = generator.generate();
    } );

    const createTable = async ( tableName: string ) => {
        try {
            console.log( `Creating table ${ tableName }` );
            await dbClient.create( {
                TableName: tableName,
                AttributeDefinitions: [
                    { AttributeName: "id", AttributeType: "S" }
                ],
                KeySchema: [
                    { AttributeName: "id", KeyType: "HASH" }
                ],
                ProvisionedThroughput: {
                    ReadCapacityUnits: 100000,
                    WriteCapacityUnits: 100000
                },
                // @note: careful with this, should not be used in production
                BillingModeSummary: {
                    BillingMode: "PAY_PER_REQUEST"
                }
            }, true );
            console.log( `Created table ${ tableName }` );
        } catch ( e ) {
            console.error( `Failed to create table ${ tableName }:`, e );
        }
    };

    async function seedTable( tableName: string, itemCount: number ): Promise<void> {
        const BATCH_SIZE = 25;
        const PARALLEL_BATCHES = 50;

        const items = [];

        // Generate all items first
        for ( let i = 0 ; i < itemCount ; i++ ) {
            item[ "id" ] = { S: i.toString() };
            items.push( { ... item } ); // Create new object to prevent reference issues
        }

        console.log( `Seeding table ${ tableName } with ${ items.length } items` );

        let insertedItems = 0;
        for ( let i = 0 ; i < items.length ; i += BATCH_SIZE * PARALLEL_BATCHES ) {
            const batchPromises = [];

            for ( let j = 0 ; j < PARALLEL_BATCHES && ( i + j * BATCH_SIZE ) < items.length ; j++ ) {
                console.log( `Inserting batch ${ j + 1 } of ${ PARALLEL_BATCHES } into ${ tableName }` );
                const batch = items.slice( i + j * BATCH_SIZE, i + ( j + 1 ) * BATCH_SIZE );
                batchPromises.push(
                    dbClient.getClient().send( new BatchWriteItemCommand( {
                        RequestItems: {
                            [ tableName ]: batch.map( item => ( {
                                PutRequest: { Item: item }
                            } ) )
                        }
                    } ) )
                );
            }

            insertedItems += batchPromises.length * BATCH_SIZE;

            // Show total in insert process
            console.log( `Inserting ${ insertedItems }/${ items.length } items into ${ tableName }` );

            await Promise.all( batchPromises );


            console.log( `Inserted ${ batchPromises.length } batches of ${ BATCH_SIZE } items into ${ tableName }` );
        }

        console.log( `Seeding ${ itemsCount } random items into each of ${ tablesCount } tables completed.` );

    }


    for ( let i = 0 ; i < tablesCount ; i++ ) {
        const tableName = generateTableOrIndexName( 4, 20 );

        await createTable( tableName );

        await seedTable( tableName, itemsCount );
    }

}
