import { analyzeAttributes } from "./commands/dyanmodb/dynamodb-analyze-attributes";
import { dynamoDBVerifyMixTypes } from "./commands/dyanmodb/dynamodb-verify-mix-types";
import { enableStreams } from "./commands/dyanmodb/dynamodb-enable-streams";
import { exportRaw } from "./commands/dyanmodb/dynamodb-export-raw";
import { exportRawItem } from "./commands/dyanmodb/dynamodb-export-raw-item";
import { exportTransform } from "./commands/dyanmodb/dynamodb-export-transform";
import { importRawData } from "./commands/dyanmodb/dynamodb-import-raw-data";
import { seed } from "./commands/dyanmodb/dynamodb-seed";
import { seedTypes } from "./commands/dyanmodb/dynamodb-seed-types";
import { mixTypes } from "./commands/dyanmodb/dynamodb-mix-types";
import { snowflakeAnalyzeTypeTransformations } from "./commands/snowflake/snowflake-analayze-transformations";

import { DynamoDBClient } from "./dynamo-db/dynamo-db-client";

import { DynamoDBLocalServer } from "./dynamo-db/dynamo-db-server";

const {
    DYNAMODB_SANDBOX_ACTIVE = "true",
    DYNAMODB_AWS_REGION = "us-east-1",
    DYNAMODB_AWS_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID",
    DYNAMODB_AWS_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY",
} = process.env;

let dbClient: DynamoDBClient;

if ( DYNAMODB_SANDBOX_ACTIVE === "true" ) {
    dbClient = DynamoDBClient.local();
} else {
    dbClient = DynamoDBClient.awsWithCredentials( DYNAMODB_AWS_REGION, {
        accessKeyId: DYNAMODB_AWS_ACCESS_KEY_ID,
        secretAccessKey: DYNAMODB_AWS_SECRET_ACCESS_KEY,
    } );
}

let dbInternalsExtractPath: string | undefined;

async function lunchDynamoDBLocal() {
    // If a client not local, then return
    if ( "true" !== DYNAMODB_SANDBOX_ACTIVE ) {
        return;
    }

    const dynamoDBLocalServer = new DynamoDBLocalServer( dbInternalsExtractPath ? {
        packageExtractPath: dbInternalsExtractPath,
    } : {} );

    await dynamoDBLocalServer.downloadInternals();

    const dbProcess = await dynamoDBLocalServer.start();

    console.log( "DynamoDB Local launched with PID:", dbProcess.pid );

    await dynamoDBLocalServer.waitForServerListening();

    console.log( "DynamoDB Local is ready." );

    return dbProcess;
}

function handleArgvBeforeStart() {
    if ( process.argv.includes( "--db-internals-path" ) ) {
        const index = process.argv.indexOf( "--db-internals-path" );
        if ( index === -1 ) {
            return;
        }

        const nextValue = process.argv[ index + 1 ];

        if ( nextValue ) {
            dbInternalsExtractPath = nextValue;
        } else {
            console.error( "Missing value for --db-internals-path" );
            process.exit( 1 );
        }
    }
}

async function handleArgvAfterStart() {
    if ( process.argv.includes( "--db-fresh-start" ) ) {
        await dbClient.dropAll()
    }
}

async function main() {
    handleArgvBeforeStart()

    const serverProcess = await lunchDynamoDBLocal();

    await handleArgvAfterStart();

    // Find an argument that starts with '@'.
    const commandIndex = process.argv.findIndex( ( arg ) => arg.startsWith( "@" ) );

    if ( commandIndex === -1 ) {
        console.error( "No command specified." );
        process.exit( 1 );
    }

    console.log( "Command:", process.argv[ commandIndex ] );

    const commandAction = process.argv[ commandIndex ];

    switch ( commandAction ) {
        case "@no-action":
            break;

        case "@dynamodb-list-tables":
            const tableNames = await dbClient.list();

            if ( ! tableNames?.length ) {
                console.log( "No tables found." );
                return;
            }

            console.log( tableNames.join( ", " ) );
            break;

        case "@dynamodb-server-run":
            if ( ! serverProcess ) {
                console.error( "Ops something went wrong." );
                return;
            }
            // Await for server shutdown before continuing
            await new Promise<void>( ( resolve ) => {
                serverProcess.once( "exit", () => {
                    console.log( "Server exited." );

                    resolve();
                } )
            } )
            return;

        case "@dynamodb-seed":
            await seed( dbClient, commandIndex )
            break;

        case "@dynamodb-seed-types":
            await seedTypes( dbClient );
            break;

        case "@dynamodb-mix-types":
            await mixTypes( dbClient );
            break;

        case "@dynamodb-verify-mix-types":
            await dynamoDBVerifyMixTypes( dbClient );
            break;

        case "@dynamodb-export-packed-data":
            await exportTransform( dbClient );
            break;

        case "@dynamodb-export-unpacked-data":
            await exportTransform( dbClient, true );
            break

        case "@dynamodb-export-raw":
            await exportRaw( dbClient );
            break;

        case "@dynamodb-export-raw-item":
            await exportRawItem( dbClient, commandIndex );
            break;

        case "@dynamodb-import":
            await importRawData( dbClient, commandIndex );
            break;

        case "@dynamodb-enable-streams":
            await enableStreams( dbClient );
            break;

        case "@dynamodb-analyze-attributes":
            await analyzeAttributes( dbClient, commandIndex );
            break;

        case "@snowfalke-analyze-transformations":
            await snowflakeAnalyzeTypeTransformations( dbClient );
            break;


        default:
            console.error( "Unknown command: " + commandAction );
    }

    serverProcess?.kill( "SIGTERM" );
}


await main().catch( console.error )

