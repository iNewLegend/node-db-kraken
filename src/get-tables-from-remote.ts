import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { dDBListAndSaveTables } from "./dynamo-db/dynamo-db-util.ts";

const client = new DynamoDBClient( {
    region: "us-east-1",
    credentials: {
        accessKeyId: "YOUR_KEY_ID",
        secretAccessKey: "YOUR__SECRET_ACCESS_KEY"
    }
} );

await dDBListAndSaveTables( client, process.cwd() + "/tables.json" )
