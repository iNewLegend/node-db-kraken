import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import * as fs from "node:fs";
import { DynamoDBLocalServer } from "./dynamo-db/dynamo-db-server";

import {
    dDB0GetTableSchema,
    dDBCreateTablesWithData,
    dDBDropAllTables,
    dDBFetchTableData,
    dDBListTables
} from "./dynamo-db/dynamo-db-table";

const client = new DynamoDBClient( {
    credentials: {
        accessKeyId: "fakeMyKeyId",
        secretAccessKey: "fakeSecretAccessKey"
    },
    region: "fakeRegion",
    endpoint: "http://localhost:8000",
} );


async function lunchDynamoDBLocal() {
    const dynamoDBLocalServer = new DynamoDBLocalServer();

    await dynamoDBLocalServer.downloadInternals();

    const dbProcess = await dynamoDBLocalServer.start();

    console.log( "DynamoDB Local launched with PID:", dbProcess.pid );

    await dynamoDBLocalServer.waitForServerListening();

    return dbProcess;
}

async function processArgv() {
    if ( process.argv.includes( "--db-fresh-start" ) ) {
        await dDBDropAllTables( client )
    }
}

async function handleTableCreation() {
    let listTableResult = ( await dDBListTables( client ) ) !;

    if ( ! listTableResult?.length ) {
        await dDBCreateTablesWithData( client, process.cwd() + "/tables.json" );
        listTableResult = ( await dDBListTables( client ) ) !; // Re-fetch the list after table creation
    }
    return listTableResult;
}

function transformToPackedMode( data: any, partitionKey: string ) {
    return data.map( ( item: any ) => {
        const partitionValue = item[ partitionKey ];
        const restOfData = { ... item };
        delete restOfData[ partitionKey ];

        return {
            [ partitionKey ]: partitionValue,
            data: JSON.stringify( restOfData ),
        };
    } );
}

async function processAllTables() {
    const tableNames = await handleTableCreation();

    // Ensure `assets` directory exists.
    if ( ! fs.existsSync( process.cwd() + "/assets" ) ) {
        fs.mkdirSync( process.cwd() + "/assets" );
    }

    for ( const tableName of tableNames ) {
        console.log( `Processing table: ${ tableName }` );

        const partitionKey = await dDB0GetTableSchema( client, tableName );
        const tableData = await dDBFetchTableData( client, tableName );

        const packedData = transformToPackedMode( tableData, partitionKey );

        const packedDataFilePath = process.cwd() + `/assets/${ tableName }-packed-data.json`;

        fs.writeFileSync( packedDataFilePath, JSON.stringify( packedData, null, 4 ) );

        console.log( `Packed data saved to file: ${ packedDataFilePath }` );
    }
}

const serverProcess = await lunchDynamoDBLocal();

await processArgv();

await processAllTables().catch( console.error )

serverProcess.kill( "SIGTERM" );

