import { faker } from "@faker-js/faker";
import * as fs from "node:fs";

import { DynamoDBLocalServer } from "./dynamo-db/dynamo-db-server";
import { DynamoDBUtil } from "./dynamo-db/dynamo-db-util";

const dbUtil = DynamoDBUtil.local();

let dbInternalsExtractPath: string | undefined;

async function lunchDynamoDBLocal() {
    const dynamoDBLocalServer = new DynamoDBLocalServer( dbInternalsExtractPath ? {
        packageExtractPath: dbInternalsExtractPath,
    } : {} );

    await dynamoDBLocalServer.downloadInternals();

    const dbProcess = await dynamoDBLocalServer.start();

    console.log( "DynamoDB Local launched with PID:", dbProcess.pid );

    await dynamoDBLocalServer.waitForServerListening();

    console.log( "DynamoDB Local is ready." );

    return dbProcess;
}

function handleArgvBeforeStart() {
    if ( process.argv.includes( "--db-internals-path" ) ) {
        const index = process.argv.indexOf( "--db-internals-path" );
        if ( index === -1 ) {
            return;
        }

        const nextValue = process.argv[ index + 1 ];

        if ( nextValue ) {
            dbInternalsExtractPath = nextValue;
        } else {
            console.error( "Missing value for --db-internals-path" );
            process.exit( 1 );
        }
    }
}

async function handleArgvAfterStart() {
    if ( process.argv.includes( "--db-fresh-start" ) ) {
        await dbUtil.dropAll()
    }
}

async function exportPackedData() {
    function transformToPackedMode( data: any[], partitionKey: string ) {
        return data.map( ( item: any ) => {
            const partitionValue = item[ partitionKey ];

            const normalizedObject: Record<string, any> = {};

            Object.entries( item ).forEach( ( [ key, value ] ) => {
                if ( key === partitionKey ) {
                    return;
                }

                if ( typeof value === "object" ) {
                    value = JSON.stringify( value );
                }

                normalizedObject[ key ] = value;
            } );

            return {
                [ partitionKey ]: partitionValue,
                ... normalizedObject,
            };
        } );
    }

    const tableNames = await dbUtil.list();

    // Ensure `assets` directory exists.
    if ( ! fs.existsSync( process.cwd() + "/assets" ) ) {
        fs.mkdirSync( process.cwd() + "/assets" );
    }

    for ( const tableName of tableNames ?? [] ) {
        console.log( `Processing table: ${ tableName }` );

        const partitionKey = await dbUtil.getSchema( tableName );
        const tableData = await dbUtil.fetch( tableName );

        const packedData = transformToPackedMode( tableData, partitionKey );

        const packedDataFilePath = process.cwd() + `/assets/${ tableName }-packed-data.json`;

        fs.writeFileSync( packedDataFilePath, JSON.stringify( packedData, null, 4 ) );

        console.log( `Packed data saved to file: ${ packedDataFilePath }` );
    }
}

async function seed( tablesCount: number, itemsCount: number ) {
    console.log( `Seeding ${ tablesCount } tables with ${ itemsCount } items each.` );
    const seedGenerators = await import( "./dynamo-db/dynamo-db-seed-generator" );

    const { getAllGenerators, generateAttributeName, generateTableOrIndexName } = seedGenerators;

    const createTable = async ( tableName: string ) => {
        try {
            console.log( `Creating table ${ tableName }` );
            await dbUtil.create( {
                TableName: tableName,
                AttributeDefinitions: [
                    { AttributeName: "id", AttributeType: "S" }
                ],
                KeySchema: [
                    { AttributeName: "id", KeyType: "HASH" }
                ],
                ProvisionedThroughput: {
                    ReadCapacityUnits: 1,
                    WriteCapacityUnits: 1
                }
            } );
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

        await dbUtil.insert( tableName, items );
    }

    console.log( `Seeding ${ itemsCount } random items into each of ${ tablesCount } tables completed.` );

    for ( let i = 0 ; i < tablesCount ; i++ ) {
        const tableName = generateTableOrIndexName( 4, 20 );

        await createTable( tableName );

        await seedTable( tableName, itemsCount );
    }

}

async function main() {
    // Find an argument that starts with '@'.
    const commandIndex = process.argv.findIndex( ( arg ) => arg.startsWith( "@" ) );

    if ( commandIndex === -1 ) {
        console.error( "No command specified." );
        process.exit( 1 );
    }

    console.log( "Command:", process.argv[ commandIndex ] );

    const commandAction = process.argv[ commandIndex ];

    switch ( commandAction ) {
        case "@seed":
            const tablesCount = parseInt( process.argv[ commandIndex + 1 ], 10 ) || 1;
            const itemsCount = parseInt( process.argv[ commandIndex + 2 ], 10 ) || 10;

            await seed( tablesCount, itemsCount );
            break;

        case "@export-packed-data":
            await exportPackedData();
            break

        case "@no-action":
            break;

        default:
            console.error( "Unknown command: " + commandAction );
    }
}

handleArgvBeforeStart()

const serverProcess = await lunchDynamoDBLocal();

await handleArgvAfterStart();

await main().catch( console.error )

serverProcess.kill( "SIGTERM" );

