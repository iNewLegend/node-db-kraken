import { symbols, type TDynamoDBPossibleSymbols } from "./dynamo-db/dynamo-db-object";
import type { ChildProcessWithoutNullStreams } from "node:child_process";
import * as fs from "node:fs";

import { el, faker } from "@faker-js/faker";

import { DynamoDBObject } from "./dynamo-db/dynamo-db-object";

import { DynamoDBLocalServer } from "./dynamo-db/dynamo-db-server";
import { DynamoDBClient } from "./dynamo-db/dynamo-db-client";

const dbClient = DynamoDBClient.local();

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
        await dbClient.dropAll()
    }
}

async function importData( path: string ) {
    // Validate path exists.
    if ( ! fs.existsSync( path ) ) {
        console.error( `File not found: ${ path }` );
        process.exit( 1 );
    }

    const result = await dbClient.import( path );

    console.log( `Imported table(s): count: ${ result.length }, names: ${ result.join( ", " ) }` );
}

async function exportRaw() {
    const tableNames = await dbClient.list();

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
    await dbClient.export( process.cwd() + path );
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

    const tableNames = await dbClient.list();

    // Ensure `assets` directory exists.
    if ( ! fs.existsSync( process.cwd() + "/assets" ) ) {
        fs.mkdirSync( process.cwd() + "/assets" );
    }

    const transformMethod = unpackedMode ? transformToUnpackedMode : transformToPackedMode;

    if ( ! tableNames?.length ) {
        console.log( "No tables found." );
        return;
    }

    for ( const tableName of tableNames ?? [] ) {
        console.log( `Processing table: ${ tableName }` );

        const partitionKey = await dbClient.getSchema( tableName );
        const tableData = await dbClient.scan( tableName );
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

async function analyzeAttributes( commandIndex: number ) {
    const tableNameArg = process.argv[ commandIndex + 1 ];

    if ( ! tableNameArg ) {
        console.error( "Missing table name." );
        console.log( "Usage: @analyze-attributes <table-name>" );
        process.exit( 1 );
    }

    const tableNames = [];

    if ( "@list" === tableNameArg ) {
        tableNames.push( ... ( await dbClient.list() ?? [] ) );
    } else {
        tableNames.push( tableNameArg );
    }

    for ( const tableName of tableNames ) {
        let scannedItemsCount = 0;

        console.log(
            `Analyzing attributes for table ${ tableName }...`
        )

        const metrics = await dbClient.getTableMetrics( tableName );

        const { tableSizeBytes, itemCount } = metrics,
            averageItemSize = tableSizeBytes / itemCount,
            limit = Math.floor( 1 * 1024 * 1024 / averageItemSize );

        const attributes = new Map<string, Set<TDynamoDBPossibleSymbols>>;

        for await ( const batch of dbClient.scanGenerator( tableName, limit ) ) {
            scannedItemsCount += batch.length;
            console.log( `Scanning ${ batch.length } items ${ scannedItemsCount }/${ itemCount }...` );
            for ( const row of batch ) {
                for ( const [ key, value ] of Object.entries( row ) ) {
                    if ( ! attributes.has( key ) ) {
                        attributes.set( key, new Set() );
                    }

                    for ( const symbol in value ) {
                        attributes.get( key )?.add( symbol as any );
                        break;
                    }
                }
            }
        }

        // Save as JSON file.
        const targetPath = process.cwd() + `/assets/${ tableName }-attributes.json`;

        fs.writeFileSync( targetPath, JSON.stringify( {
            attributes: Array.from( attributes.entries() ).map( ( [ key, value ] ) => {
                return {
                    name: key,
                    symbols: Array.from( value ),
                };
            } ),
            averageItemSize,
            limit,
            itemCount,
            tableSizeBytes,
        }, null, 4 ) );

        console.log( `\Attributes for table ${ tableName } saved to file: ${ targetPath }` );
    }
}

async function main( localServerProcess: ChildProcessWithoutNullStreams ) {
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

        case "@export-raw-item":
            const tableName = process.argv[ commandIndex + 1 ];
            const id = process.argv[ commandIndex + 2 ];

            if ( ! tableName || ! id ) {
                console.error( "Missing table name or id." );
                console.log( "Usage: @export-raw-item <table-name> <id>" );
                process.exit( 1 );
            }

            const item = await dbClient.getItemById( tableName, id );

            if ( ! item ) {
                console.error( `Item not found: ${ tableName }#${ id }` );
                process.exit( 1 );
            }

            // Save the item in JSON format, in path: /assets/<table-name>-<id>.json
            const targetPath = process.cwd() + `/assets/${ tableName }-${ id }.json`;

            fs.writeFileSync( targetPath, JSON.stringify( item, null, 4 ) );

            console.log( `Item saved to file: ${ targetPath }` );

            break;

        case "@import":
            const path = process.argv[ commandIndex + 1 ];

            if ( ! path ) {
                console.error( "Missing path to import data from." );
                console.log( "Usage: @import <path-to-data-file>" );
                process.exit( 1 );
            }

            await importData( path );
            break;

        case "@list-tables":
            const tableNames = await dbClient.list();

            if ( ! tableNames?.length ) {
                console.log( "No tables found." );
                return;
            }

            console.log( tableNames.join( ", " ) );
            break;

        case "@no-action":
            break;

        case "@server-run":
            // Await for server shutdown before continuing
            await new Promise<void>( ( resolve ) => {
                localServerProcess.once( "exit", () => {
                    console.log( "Server exited." );

                    resolve();
                } )
            } )
            return;

        case "@analyze-attributes":
            await analyzeAttributes( commandIndex );
            break;


        default:
            console.error( "Unknown command: " + commandAction );
    }

    serverProcess.kill( "SIGTERM" );
}

handleArgvBeforeStart()

const serverProcess = await lunchDynamoDBLocal();

await handleArgvAfterStart();

await main( serverProcess ).catch( console.error )


