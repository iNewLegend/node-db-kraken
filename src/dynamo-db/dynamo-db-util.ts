import {
    CreateTableCommand,
    DeleteTableCommand,
    DescribeTableCommand,
    DynamoDBClient,
    ListTablesCommand,
    PutItemCommand,
    ScanCommand
} from "@aws-sdk/client-dynamodb";

import type { TableDescription } from "@aws-sdk/client-dynamodb/dist-types";

import * as fs from 'fs/promises';
import * as util from "node:util";

const debug = util.debug( 'dynamodb-local:util' );

const readFileAndParseJSON = async ( filePath: string ) => {
    const fileContent = await fs.readFile( filePath, 'utf-8' );
    return JSON.parse( fileContent );
};

function convertToUint8Array( data: any ): Uint8Array {
    return new Uint8Array( Object.values( data ) );
}

export async function dDBListTables( client: DynamoDBClient ) {
    const command = new ListTablesCommand( {} );

    debug( "Listing all tables..." );

    const response = await client.send( command );

    debug( "Tables found:", response.TableNames );

    return response.TableNames;
}

export async function dDBDescribeTable( client: DynamoDBClient, tableName: string ) {
    const command = new DescribeTableCommand( { TableName: tableName } );

    debug( `Describing table: ${ tableName }...` );

    const response = await client.send( command );

    debug( `Table description for ${ tableName }:`, response.Table );

    return response.Table;
}

export async function dDBSaveTables( tables: TableDescription[], filePath: string ) {
    debug( "Saving tables to file:", filePath );

    await fs.writeFile( filePath, JSON.stringify( tables, null, 2 ) );

    debug( "Tables saved successfully." );
}

export async function dDBListAndSaveTables( client: DynamoDBClient, filePath: string ) {
    try {
        const tableNames = await dDBListTables( client );

        if ( ! tableNames || tableNames.length === 0 ) {
            debug( "No tables found to list and save." );
            return;
        }

        const tableDetails = await Promise.all( tableNames.map( async ( tableName ) => {
            const description = await dDBDescribeTable( client, tableName );
            const data = await dDBFetchTableData( client, tableName );

            if ( ! description || ! data ) {
                throw new Error( `Failed to get table description or data for table ${ tableName }` );
            }

            return {
                ... description,
                data
            };
        } ) );

        debug( "Saving table descriptions and data to file:", filePath );

        await dDBSaveTables( tableDetails, filePath );

        debug( "Tables with data saved successfully." );
    } catch ( error ) {
        console.error( "Failed to list and save tables:", error );
    }
}

async function dDBInsertDataIntoTable( client: DynamoDBClient, tableName: string, items: any[] ) {
    for ( const item of items ) {
        for ( const key in item ) {
            if ( item[ key ].B ) {
                item[ key ].B = convertToUint8Array( item[ key ].B );
            }
            if ( item[ key ].BS ) {
                item[ key ].BS = item[ key ].BS.map( convertToUint8Array );
            }
        }
        const command = new PutItemCommand( { TableName: tableName, Item: item } );

        await client.send( command );

        debug( `Inserted item into ${ tableName }:`, item );
    }
}

export async function dDBCreateTables( client: DynamoDBClient, filePath: string ) {
    try {
        const tables = await readFileAndParseJSON( filePath );

        for ( const table of tables ) {
            const createTableParams = {
                TableName: table.TableName,
                AttributeDefinitions: table.AttributeDefinitions,
                KeySchema: table.KeySchema,
                ProvisionedThroughput: table.ProvisionedThroughput,
                StreamSpecification: table.StreamSpecification,
                TableClass: table.TableClassSummary?.TableClass
            };

            debug( "Creating table:", table.TableName );

            const command = new CreateTableCommand( createTableParams );
            const response = await client.send( command );

            debug( `Table ${ table.TableName } created successfully.`, response.TableDescription );
        }
    } catch ( error ) {
        console.error( "Failed to create tables:", error );
    }
}

export async function dDBCreateTablesWithData( client: DynamoDBClient, filePath: string ) {
    try {
        await dDBCreateTables( client, filePath );

        const tables = await readFileAndParseJSON( filePath );

        for ( const table of tables ) {
            if ( table.data && table.data.length > 0 ) {
                debug( `Inserting data into ${ table.TableName }` );

                await dDBInsertDataIntoTable( client, table.TableName, table.data );

                debug( `Data inserted into ${ table.TableName }` );
            }
        }
    } catch ( error ) {
        console.error( "Failed to create tables and insert data:", error );
    }
}

export async function dDBGetTableSchema( client: DynamoDBClient, tableName: string ) {
    const command = new DescribeTableCommand( { TableName: tableName } );

    debug( `Getting schema for table: ${ tableName }` );

    const { Table } = await client.send( command );
    const partitionKey = Table?.KeySchema?.find( key => key.KeyType === "HASH" )?.AttributeName ?? "";

    debug( `Partition key for table ${ tableName }: ${ partitionKey }` );

    return partitionKey;
}

export async function dDBFetchTableData( client: DynamoDBClient, tableName: string ) {
    const command = new ScanCommand( { TableName: tableName } );

    debug( `Fetching data for table: ${ tableName }` );

    const response = await client.send( command );

    debug( `Data fetched for table ${ tableName }:`, response.Items );

    return response.Items ?? [];
}

export async function dDBDropTable( client: DynamoDBClient, tableName: string ) {
    const command = new DeleteTableCommand( { TableName: tableName } );

    debug( `Dropping table: ${ tableName }...` );

    const response = await client.send( command );

    debug( `Table dropped successfully: ${ tableName }` );

    return response;
}

export async function dDBDropAllTables( client: DynamoDBClient ) {
    try {
        const tableNames = await dDBListTables( client );
        if ( ! tableNames || tableNames.length === 0 ) {
            debug( "No tables found to drop." );
            return;
        }

        const results = await Promise.all( tableNames.map( name => dDBDropTable( client, name ) ) );

        debug( "All tables dropped successfully." );

        return results;
    } catch ( error ) {
        console.error( "Failed to drop all tables:", error );
    }
}

