import { faker } from "@faker-js/faker";
import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";

export async function seed( dbClient: DynamoDBClient, commandIndex: number ) {
    const tablesCount = parseInt( process.argv[ commandIndex + 1 ], 10 ) || 1;
    const itemsCount = parseInt( process.argv[ commandIndex + 2 ], 10 ) || 10;


    console.log( `Seeding ${ tablesCount } tables with ${ itemsCount } items each.` );
    const seedGenerators = await import( "../../dynamo-db/dynamo-db-seed-generator" );

    const { getAllGenerators, generateAttributeName, generateTableOrIndexName } = seedGenerators;

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
                    ReadCapacityUnits: 1000,
                    WriteCapacityUnits: 1000
                }
            }, true );
            console.log( `Created table ${ tableName }` );
        } catch ( e ) {
            console.error( `Failed to create table ${ tableName }:`, e );
        }
    };

    async function seedTable( tableName: string, itemCount: number ): Promise<void> {
        const items = [];

        console.log( `Seeding ${ itemCount } random items into table ${ tableName }` );

        for ( let i = 0 ; i < itemCount ; i++ ) {
            const item: any = {};

            getAllGenerators().forEach( ( generator ) => {
                item[ generateAttributeName() ] = generator.generate();
            } );

            item[ "id" ] = { S: faker.string.uuid() };

            items.push( item );
        }

        await dbClient.insert( tableName, items );
    }

    console.log( `Seeding ${ itemsCount } random items into each of ${ tablesCount } tables completed.` );

    for ( let i = 0 ; i < tablesCount ; i++ ) {
        const tableName = generateTableOrIndexName( 4, 20 );

        await createTable( tableName );

        await seedTable( tableName, itemsCount );
    }

}
