import fs from "node:fs";
import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";

export async function dynamoDBexportRaw( dbClient: DynamoDBClient ) {
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
