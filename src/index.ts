import { CreateTableCommand, DynamoDBClient, ListTablesCommand } from "@aws-sdk/client-dynamodb";

import {
    dDBDownload,
    dDBEnsurePortActivity,
    dDBHandleTermination,
    dDBLaunch,
} from "./dynamo-db/dynamo-db";
import {
    dDB0GetTableSchema,
    dDBCreateTables, dDBCreateTablesWithData,
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
    await dDBDownload();

    const dbProcess = await dDBLaunch();

    console.log( "DynamoDB Local launched with PID:", dbProcess.pid );

    await dDBEnsurePortActivity();

    dDBHandleTermination( dbProcess.pid! );

    return dbProcess;
}

await lunchDynamoDBLocal();

if ( process.argv.includes( "--fresh-start" ) ) {
    await dDBDropAllTables( client )
}


// List exist tables to ensure connection
let listTableResult = ( await dDBListTables( client ) ) !;

if ( ! listTableResult?.length ) {
    await dDBCreateTablesWithData( client, process.cwd() + "/tables.json" );
    listTableResult = ( await dDBListTables( client ) ) !; // Re-fetch the list after table creation
}

// Function to transform data to packed mode
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

// Main function to process all tables
async function processAllTables() {
    const tableNames = listTableResult;

    for ( const tableName of tableNames ) {
        console.log( `Processing table: ${ tableName }` );

        const partitionKey = await dDB0GetTableSchema( client, tableName );
        const tableData = await dDBFetchTableData( client, tableName );

        console.log( `Table Data for table ${ tableName }:`, JSON.stringify( tableData, null, 2 ) );

        const packedData = transformToPackedMode( tableData, partitionKey );

        console.log( `Packed Data for table ${ tableName }:`, JSON.stringify( packedData, null, 2 ) );
    }
}

processAllTables().catch( console.error );
