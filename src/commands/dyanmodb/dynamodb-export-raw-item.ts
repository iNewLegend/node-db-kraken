import fs from "node:fs";
import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";

export async function dynamoDBexportRawItem( dbClient: DynamoDBClient, commandIndex: number ) {
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
}
