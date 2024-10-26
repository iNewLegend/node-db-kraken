import fs from "node:fs";
import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";

export async function importRawData( dbClient: DynamoDBClient, commandIndex: number ) {
    const path = process.argv[ commandIndex + 1 ];

    if ( ! path ) {
        console.error( "Missing path to import data from." );
        console.log( "Usage: @import <path-to-data-file>" );
        process.exit( 1 );
    }

    // Validate path exists.
    if ( ! fs.existsSync( path ) ) {
        console.error( `File not found: ${ path }` );
        process.exit( 1 );
    }

    const result = await dbClient.import( path );

    console.log( `Imported table(s): count: ${ result.length }, names: ${ result.join( ", " ) }` );
}
