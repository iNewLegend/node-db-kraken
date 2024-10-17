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

const debug = util.debug( 'dynamodb-local:table' );

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

        const tableDetails = await Promise.all(
            tableNames.map( async ( tableName ) => {
                const description = await dDBDescribeTable( client, tableName );
                const data = await dDBFetchTableData( client, tableName );

                if ( ! description || ! data ) {
                    throw new Error( `Failed to get table description or data for table ${ tableName }` );
                }

                if ( ! description.ProvisionedThroughput ) {
                    throw new Error( `No provisioned throughput for table ${ tableName }` );
                }

                const fullDescription = {
                    TableName: description.TableName,
                    AttributeDefinitions: description.AttributeDefinitions,
                    KeySchema: description.KeySchema,
                    ProvisionedThroughput: {
                        ReadCapacityUnits: description.ProvisionedThroughput.ReadCapacityUnits,
                        WriteCapacityUnits: description.ProvisionedThroughput.WriteCapacityUnits
                    },
                    StreamSpecification: description.StreamSpecification,
                    TableClassSummary: description.TableClassSummary ? { TableClass: description.TableClassSummary.TableClass } : undefined,
                    // Add any additional properties you might require
                    data
                };

                return fullDescription;
            } )
        );

        debug( "Saving table descriptions and data to file:", filePath );
        await fs.writeFile( filePath, JSON.stringify( tableDetails, null, 2 ) );
        debug( "Tables with data saved successfully." );
    } catch ( error ) {
        console.error( "Failed to list and save tables:", error );
    }
}

function convertToUint8Array( data: any ): Uint8Array {
    return new Uint8Array( Object.values( data ) );
}

async function dDBInsertDataIntoTable( client: DynamoDBClient, tableName: string, items: any[] ) {
    for ( const item of items ) {
        // Adjust binary attributes to Uint8Array
        for ( const key in item ) {
            if ( item[ key ].B ) {
                item[ key ].B = convertToUint8Array( item[ key ].B );
            }

            if ( item[ key ].BS ) {
                item[ key ].BS = item[ key ].BS.map( ( binaryItem: any ) => convertToUint8Array( binaryItem ) );
            }
        }

        const command = new PutItemCommand( {
            TableName: tableName,
            Item: item
        } );
        await client.send( command );
        debug( `Inserted item into ${ tableName }:`, item );
    }
}


export async function dDBCreateTables( client: DynamoDBClient, filePath: string ) {
    try {
        // Read the table descriptions JSON file
        debug( "Reading table descriptions from file:", filePath );
        const fileContent = await fs.readFile( filePath, 'utf-8' );
        debug( "File content read successfully." );

        const tables = JSON.parse( fileContent );
        debug( "Table descriptions parsed:", util.inspect( tables, {
            depth: 2,
            colors: true,
            compact: true
        } ) );

        // Iterate over the tables and create them
        for ( const table of tables ) {
            const createTableParams = {
                TableName: table.TableName,
                AttributeDefinitions: table.AttributeDefinitions,
                KeySchema: table.KeySchema,
                ProvisionedThroughput: {
                    ReadCapacityUnits: table.ProvisionedThroughput.ReadCapacityUnits,
                    WriteCapacityUnits: table.ProvisionedThroughput.WriteCapacityUnits
                },
                StreamSpecification: table.StreamSpecification,
                TableClass: table.TableClassSummary.TableClass
            };

            debug( "Creating table:", table.TableName );
            const command = new CreateTableCommand( createTableParams );
            const response = await client.send( command );

            if ( ! response.TableDescription ) {
                throw new Error( `No table description returned for table ${ table.TableName }` );
            }

            debug( `Table ${ table.TableName } created successfully.` );
        }
    } catch ( error ) {
        console.error( "Failed to create tables:", error );
    }
}

export async function dDBCreateTablesWithData( client: DynamoDBClient, filePath: string ) {
    try {
        // Call the existing dDBCreateTables function
        await dDBCreateTables( client, filePath );

        // Read the table descriptions JSON file
        const fileContent = await fs.readFile( filePath, 'utf-8' );
        const tables = JSON.parse( fileContent );

        // Iterate over the tables to insert test data if provided
        // Iterate over the tables to insert data if provided
        for ( const table of tables ) {
            if ( table.data && table.data.length > 0 ) {
                debug( `Inserting data into ${ table.TableName }` );
                await dDBInsertDataIntoTable( client, table.TableName, table.data );
                debug( `Data inserted into ${ table.TableName }` );
            }
        }
    } catch ( error ) {
        console.error( "Failed to create tables and insert test data:", error );
    }
}

export async function dDB0GetTableSchema( client: DynamoDBClient, tableName: string ) {
    const describeTableCommand = new DescribeTableCommand( { TableName: tableName } );

    debug( `Getting schema for table: ${ tableName }` );
    const tableDescription = await client.send( describeTableCommand );
    const keySchema = tableDescription.Table?.KeySchema?.find( key => key.KeyType === "HASH" );

    const partitionKeyName = keySchema?.AttributeName ?? "";
    debug( `Partition key for table ${ tableName }: ${ partitionKeyName }` );

    return partitionKeyName;
}

export async function dDBFetchTableData( client: DynamoDBClient, tableName: string ) {
    const scanCommand = new ScanCommand( { TableName: tableName } );

    debug( `Fetching data for table: ${ tableName }` );
    const tableData = await client.send( scanCommand );

    debug( `Data fetched for table ${ tableName }:`, tableData.Items );
    return tableData.Items ?? [];
}

export async function dDBDropTable( client: DynamoDBClient, tableName: string ) {
    const deleteCommand = new DeleteTableCommand( { TableName: tableName } );

    debug( `Dropping table: ${ tableName }...` );
    const response = await client.send( deleteCommand );

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

        const dropResults = await Promise.all(
            tableNames.map( async ( tableName ) => {
                return await dDBDropTable( client, tableName );
            } )
        );

        debug( "All tables dropped successfully." );
        return dropResults;
    } catch ( error ) {
        console.error( "Failed to drop all tables:", error );
    }
}
