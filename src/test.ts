// Usage Example
import { DynamoDBClient } from "./dynamo-db/dynamo-db-client";
import { DynamoDBStreams } from "./dynamo-db/dynamo-db-streams";
import { DynamoDBSyncManager } from "./dynamo-db/dynamo-db-sync.manager";

const dynamoDbClient = DynamoDBClient.awsWithCredentials( "us-east-1", {
    accessKeyId: "",
    secretAccessKey: "",
} );

const syncManager = new DynamoDBSyncManager( dynamoDbClient );

const generator = syncManager.getRecords(
    "table1",
    await dynamoDbClient.getTableMetrics( "table1" ), {
        useCache: true
    }
)

for await ( const batch of generator ) {
}
