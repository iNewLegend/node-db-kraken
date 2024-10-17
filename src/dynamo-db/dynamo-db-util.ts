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

export class DynamoDBUtil {
    private client: DynamoDBClient;

    public constructor( client: DynamoDBClient ) {
        this.client = client;
    }

    private async readFileAndParseJSON( filePath: string ) {
        const fileContent = await fs.readFile( filePath, 'utf-8' );
        return JSON.parse( fileContent );
    }

    private convertToUint8Array( data: any ): Uint8Array {
        return new Uint8Array( Object.values( data ) );
    }

    async listTables() {
        const command = new ListTablesCommand( {} );

        debug( "Listing all tables..." );

        const response = await this.client.send( command );

        debug( "Tables found:", response.TableNames );

        return response.TableNames;
    }

    async describeTable( tableName: string ) {
        const command = new DescribeTableCommand( { TableName: tableName } );

        debug( `Describing table: ${ tableName }...` );

        const response = await this.client.send( command );

        debug( `Table description for ${ tableName }:`, response.Table );

        return response.Table;
    }

    async saveTables( tables: TableDescription[], filePath: string ) {
        debug( "Saving tables to file:", filePath );

        await fs.writeFile( filePath, JSON.stringify( tables, null, 2 ) );

        debug( "Tables saved successfully." );
    }

    async listAndSaveTables( filePath: string ) {
        try {
            const tableNames = await this.listTables();

            if ( ! tableNames || tableNames.length === 0 ) {
                debug( "No tables found to list and save." );
                return;
            }

            const tableDetails = await Promise.all( tableNames.map( async ( tableName ) => {
                const description = await this.describeTable( tableName );
                const data = await this.fetchTableData( tableName );

                if ( ! description || ! data ) {
                    throw new Error( `Failed to get table description or data for table ${ tableName }` );
                }

                return {
                    ... description,
                    data
                };
            } ) );

            debug( "Saving table descriptions and data to file:", filePath );

            await this.saveTables( tableDetails, filePath );

            debug( "Tables with data saved successfully." );
        } catch ( error ) {
            console.error( "Failed to list and save tables:", error );
        }
    }

    async insertDataIntoTable( tableName: string, items: any[] ) {
        for ( const item of items ) {
            for ( const key in item ) {
                if ( item[ key ].B ) {
                    item[ key ].B = this.convertToUint8Array( item[ key ].B );
                }
                if ( item[ key ].BS ) {
                    item[ key ].BS = item[ key ].BS.map( this.convertToUint8Array );
                }
            }
            const command = new PutItemCommand( { TableName: tableName, Item: item } );

            await this.client.send( command );

            debug( `Inserted item into ${ tableName }:`, item );
        }
    }

    async createTables( filePath: string ) {
        try {
            const tables = await this.readFileAndParseJSON( filePath );

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
                const response = await this.client.send( command );

                debug( `Table ${ table.TableName } created successfully.`, response.TableDescription );
            }
        } catch ( error ) {
            console.error( "Failed to create tables:", error );
        }
    }

    async createTablesWithData( filePath: string ) {
        try {
            await this.createTables( filePath );

            const tables = await this.readFileAndParseJSON( filePath );

            for ( const table of tables ) {
                if ( table.data && table.data.length > 0 ) {
                    debug( `Inserting data into ${ table.TableName }` );

                    await this.insertDataIntoTable( table.TableName, table.data );

                    debug( `Data inserted into ${ table.TableName }` );
                }
            }
        } catch ( error ) {
            console.error( "Failed to create tables and insert data:", error );
        }
    }

    async getTableSchema( tableName: string ) {
        const command = new DescribeTableCommand( { TableName: tableName } );

        debug( `Getting schema for table: ${ tableName }` );

        const { Table } = await this.client.send( command );
        const partitionKey = Table?.KeySchema?.find( key => key.KeyType === "HASH" )?.AttributeName ?? "";

        debug( `Partition key for table ${ tableName }: ${ partitionKey }` );

        return partitionKey;
    }

    async fetchTableData( tableName: string ) {
        const command = new ScanCommand( { TableName: tableName } );

        debug( `Fetching data for table: ${ tableName }` );

        const response = await this.client.send( command );

        debug( `Data fetched for table ${ tableName }:`, response.Items );

        return response.Items ?? [];
    }

    async dropTable( tableName: string ) {
        const command = new DeleteTableCommand( { TableName: tableName } );

        debug( `Dropping table: ${ tableName }...` );

        const response = await this.client.send( command );

        debug( `Table dropped successfully: ${ tableName }` );

        return response;
    }

    async dropAllTables() {
        try {
            const tableNames = await this.listTables();
            if ( ! tableNames || tableNames.length === 0 ) {
                debug( "No tables found to drop." );
                return;
            }

            const results = await Promise.all( tableNames.map( name => this.dropTable( name ) ) );

            debug( "All ta¬bles dropped successfully." );

            return results;
        } catch ( error ) {
            console.error( "Failed to drop all tables:", error );
        }
    }
}

