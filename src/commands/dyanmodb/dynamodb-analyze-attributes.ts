import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";
import type { TDynamoDBPossibleSymbols } from "../../dynamo-db/dynamo-db-object";
import fs from "node:fs";

export async function analyzeAttributes( dbClient: DynamoDBClient, commandIndex: number ) {
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
