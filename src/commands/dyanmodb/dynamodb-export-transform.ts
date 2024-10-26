import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";
import { DynamoDBObject } from "../../dynamo-db/dynamo-db-object";
import fs from "node:fs";

export async function exportTransform( dbClient: DynamoDBClient, unpackedMode = false ) {
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
