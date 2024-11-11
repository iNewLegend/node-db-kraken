import { ScanCommand } from "@aws-sdk/client-dynamodb";
import fs from "node:fs";
import path from "path";
import { DynamoDBLocalCacheStrategy } from "../../dynamo-db/cache-strategies/dynamodb-local-cache-strategy";
import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";
import { DynamoDBSyncManager } from "../../dynamo-db/dynamodb-sync-manager";


export async function dynamodbCacheSync( dbClient: DynamoDBClient ) {

    const cacheStrategy = new DynamoDBLocalCacheStrategy();

    const syncManager = new DynamoDBSyncManager(
        dbClient,
        cacheStrategy,
    );

    const client = dbClient.getClient();

    const targetTable = 'type-test-B-M-NULL';

    const metrics = await dbClient.getTableMetrics( targetTable );

    let temp = 0;
    const startTime = Date.now();
    // for await ( const batch of syncManager.getRecords( "WSmVLwfXRBuVdmLLJ", metrics, { useCache: true } ) ) {
    //     if ( "undefined" !== typeof batch.segment ) {
    //         console.log( 'segment: ', batch.segment );
    //         temp += batch.meta.scanned;
    //     }
    // }
    for await ( const batch of dbClient.scanParallel( targetTable, metrics ) ) {
        if ( "undefined" !== typeof batch.segment ) {
            console.log( 'segment: ', batch.segment );
            temp += batch.meta.scanned;
        }
    }
    console.log( 'time: ', Date.now() - startTime );
    console.log( `total records: ${ temp }` );
    //
    // const limit = dbClient.getApproximateItemsLimit( metrics );
    //
    // const records = [];
    //
    // // Simple scan with 20 items
    // const signal = await client.send( new ScanCommand( {
    //     TableName: '4lLekcylpRbdC.R4.sUA',
    //     Limit: 10
    // } ) )
    //
    //
    // const signal2 = await client.send( new ScanCommand( {
    //     TableName: '4lLekcylpRbdC.R4.sUA',
    //     Limit: 10,
    //     ExclusiveStartKey: signal.LastEvaluatedKey,
    // } ) )
    //
    // console.log( [ ... signal.Items!, ... signal2.Items! ]!.map( i => i.id.S ) );
    //
    // for await ( const item of readDynamoDBCache() ) {
    //     if ( item.type === 'header' ) {
    //         console.log( 'Header:', item.data );
    //     } else {
    //         console.log( 'Record:', item.data );
    //     }
    // }
}

