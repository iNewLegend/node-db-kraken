import { faker } from "@faker-js/faker";
import * as fs from "node:fs";
import { DynamoDBObject } from "./dynamo-db/dynamo-db-object";

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

async function exportRaw() {
    const tableNames = await dbUtil.list();

    if ( ! tableNames?.length ) {
        console.log( "No tables found." );
        return;
    }

    // Ensure `assets` directory exists.
    if ( ! fs.existsSync( process.cwd() + "/assets" ) ) {
        fs.mkdirSync( process.cwd() + "/assets" );
    }

    const path = `/assets/${ new Date().toISOString().split( "T" )[ 0 ] }-raw-data.json`;

    console.log( `Table names: ${ tableNames.join( ", " ) }` );
    console.log( `Exporting raw data to ${ process.cwd() + path }` );

    // Export to assets with date format eg: 2024-05-05
    await dbUtil.export( process.cwd() + path );
}

async function exportTransform( unpackedMode = false ) {
    function transformToPackedMode( data: any[], partitionKey: string ) {
        return data.map( ( item: any ) => {
            const partitionValue = DynamoDBObject.from( item[ partitionKey ], 0 );

            delete item[ partitionKey ];

            const parsedObject = DynamoDBObject.from( { M: { ... item } }, 0 );

            return {
                [ partitionKey ]: partitionValue,
                data: parsedObject,
            };
        } );
    }

    function transformToUnpackedMode( table: any[], partitionKey: string ) {
        return table.map( ( row: any ) => {
            const parsedObject = DynamoDBObject.from( { M: row }, 1 );
            const partitionValue = parsedObject[ partitionKey ];

            delete parsedObject[ partitionKey ];

            return {
                [ partitionKey ]: partitionValue,
                ... parsedObject,
            };
        } );
    }

    const tableNames = await dbUtil.list();

    // Ensure `assets` directory exists.
    if ( ! fs.existsSync( process.cwd() + "/assets" ) ) {
        fs.mkdirSync( process.cwd() + "/assets" );
    }

    const transformMethod = unpackedMode ? transformToUnpackedMode : transformToPackedMode;

    for ( const tableName of tableNames ?? [] ) {
        console.log( `Processing table: ${ tableName }` );

        const partitionKey = await dbUtil.getSchema( tableName );
        const tableData = await dbUtil.fetch( tableName );
        const packedData = transformMethod( tableData, partitionKey );

        const format = `/assets/${ tableName }-${ unpackedMode ? "unpacked" : "packed" }-data.json`;

        const targetPath = process.cwd() + format;

        fs.writeFileSync( targetPath, JSON.stringify( packedData, null, 4 ) );

        console.log( `data saved to file: ${ targetPath }` );
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
                    ReadCapacityUnits: 10,
                    WriteCapacityUnits: 10
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
            await exportTransform();
            break;
        case "@export-unpacked-data":
            await exportTransform( true );
            break

        case "@export-raw":
            await exportRaw();
            break;


        case "@list-tables":
            const tableNames = await dbUtil.list();

            if ( ! tableNames?.length ) {
                console.log( "No tables found." );
                return;
            }

            console.log( tableNames.join( ", " ) );
            break;

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

