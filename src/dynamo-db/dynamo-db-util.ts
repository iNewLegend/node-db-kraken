import {
    CreateTableCommand,
    DeleteTableCommand,
    type DeleteTableCommandOutput,
    DescribeTableCommand,
    DynamoDBClient,
    type GlobalSecondaryIndex,
    ListTablesCommand,
    type LocalSecondaryIndex,
    type ProvisionedThroughput,
    PutItemCommand,
    ScanCommand
} from "@aws-sdk/client-dynamodb";

import type { TableDescription } from "@aws-sdk/client-dynamodb/dist-types";
import type { ListTablesInput } from "@aws-sdk/client-dynamodb/dist-types/models/models_0";

import * as fs from 'fs/promises';
import * as util from "node:util";

util.inspect.defaultOptions.depth = 10;

const debug = util.debug( 'dynamodb-local:util' );

export class DynamoDBUtil {
    private client: DynamoDBClient;

    public static local() {
        const client = new DynamoDBClient( {
            credentials: {
                accessKeyId: "fakeMyKeyId",
                secretAccessKey: "fakeSecretAccessKey"
            },
            region: "fakeRegion",
            endpoint: "http://localhost:8000",
        } );

        return new DynamoDBUtil( client );
    }

    public constructor( client: DynamoDBClient ) {
        this.client = client;
    }

    private convertTableDescriptionToCreateTableInput(
        tableDescription: TableDescription
    ) {
        const provisionedThroughput: ProvisionedThroughput | undefined = tableDescription.ProvisionedThroughput
            ? {
                ReadCapacityUnits: tableDescription.ProvisionedThroughput.ReadCapacityUnits!,
                WriteCapacityUnits: tableDescription.ProvisionedThroughput.WriteCapacityUnits!,
            }
            : undefined;

        const globalSecondaryIndexes: GlobalSecondaryIndex[] | undefined =
            tableDescription.GlobalSecondaryIndexes?.map( index => ( {
                IndexName: index.IndexName!,
                KeySchema: index.KeySchema,
                Projection: index.Projection,
                ProvisionedThroughput: index.ProvisionedThroughput
                    ? {
                        ReadCapacityUnits: index.ProvisionedThroughput.ReadCapacityUnits!,
                        WriteCapacityUnits: index.ProvisionedThroughput.WriteCapacityUnits!,
                    }
                    : undefined,
            } ) );

        const localSecondaryIndexes: LocalSecondaryIndex[] | undefined =
            tableDescription.LocalSecondaryIndexes?.map( index => ( {
                IndexName: index.IndexName!,
                KeySchema: index.KeySchema,
                Projection: index.Projection,
            } ) );

        return {
            TableName: tableDescription.TableName,
            AttributeDefinitions: tableDescription.AttributeDefinitions,
            KeySchema: tableDescription.KeySchema,
            ProvisionedThroughput: provisionedThroughput,
            GlobalSecondaryIndexes: globalSecondaryIndexes,
            LocalSecondaryIndexes: localSecondaryIndexes,
            StreamSpecification: tableDescription.StreamSpecification,
            SSESpecification: tableDescription.SSEDescription
                ? {
                    Enabled: tableDescription.SSEDescription.Status === 'ENABLED',
                    SSEType: tableDescription.SSEDescription.SSEType,
                    KMSMasterKeyId: tableDescription.SSEDescription.KMSMasterKeyArn,
                }
                : undefined,
            BillingMode: tableDescription.BillingModeSummary
                ? tableDescription.BillingModeSummary.BillingMode
                : undefined,
            // Remove Tags property as it does not exist on TableDescription
        };
    }


    private async loadTables( filePath: string ) {
        const fileContent = await fs.readFile( filePath, 'utf-8' );
        return JSON.parse( fileContent );
    }

    private async saveTables( tables: TableDescription[], filePath: string ) {
        debug( "Saving tables to file:", filePath );

        await fs.writeFile( filePath, JSON.stringify( tables, null, 2 ) );

        debug( "Tables saved successfully." );
    }

    private convertToUint8Array( data: any ): Uint8Array {
        return new Uint8Array( Object.values( data ) );
    }

    async list( input: ListTablesInput = {} ) {
        const command = new ListTablesCommand( input );

        debug( "Listing tables...", input );

        const response = await this.client.send( command );

        debug( "Tables found:", response.TableNames );

        return response.TableNames;
    }

    async describe( tableName: string ) {
        const command = new DescribeTableCommand( { TableName: tableName } );

        debug( `Describing table: ${ tableName }...` );

        const response = await this.client.send( command );

        debug( `Table description for ${ tableName }:`, response.Table );

        return response.Table;
    }

    async insert( tableName: string, items: any[] ) {
        for ( const item of items ) {
            // for ( const key in item ) {
            //     if ( item[ key ].B ) {
            //         item[ key ].B = this.convertToUint8Array( item[ key ].B );
            //     }
            //     if ( item[ key ].BS ) {
            //         item[ key ].BS = item[ key ].BS.map( this.convertToUint8Array );
            //     }
            // }
            debug( `Inserting item into ${ tableName }:`, item );

            const command = new PutItemCommand( { TableName: tableName, Item: item } );

            await this.client.send( command );

            debug( `Inserted item into ${ tableName }:`, item );
        }
    }

    async export( filePath: string ) {
        const tableNames = await this.list();

        if ( ! tableNames || tableNames.length === 0 ) {
            debug( "No tables found to list and save." );
            return;
        }

        const tableDetails = await Promise.all( tableNames.map( async ( tableName ) => {
            const description = await this.describe( tableName );
            const data = await this.fetch( tableName );

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
    }

    async importSchema( filePath: string ) {
        const tables = await this.loadTables( filePath );

        for ( const table of tables ) {
            await this.create( table );
        }
    }

    async import( filePath: string ) {
        await this.importSchema( filePath );

        const tables = await this.loadTables( filePath );

        for ( const table of tables ) {
            if ( table.data && table.data.length > 0 ) {
                debug( `Inserting data into ${ table.TableName }` );

                await this.insert( table.TableName, table.data );

                debug( `Data inserted into ${ table.TableName }` );
            }
        }
    }

    public async create( table: TableDescription ) {
        debug( "Creating table:", table.TableName );

        const command = new CreateTableCommand( this.convertTableDescriptionToCreateTableInput( table ) );
        const response = await this.client.send( command );

        debug( `Table ${ table.TableName } created successfully.`, response.TableDescription );
    }

    async getSchema( tableName: string ) {
        const command = new DescribeTableCommand( { TableName: tableName } );

        debug( `Getting schema for table: ${ tableName }` );

        const { Table } = await this.client.send( command );
        const partitionKey = Table?.KeySchema?.find( key => key.KeyType === "HASH" )?.AttributeName ?? "";

        debug( `Partition key for table ${ tableName }: ${ partitionKey }` );

        return partitionKey;
    }

    async fetch( tableName: string ) {
        const command = new ScanCommand( { TableName: tableName } );

        debug( `Fetching data for table: ${ tableName }` );

        const response = await this.client.send( command );

        debug( `Data fetched for table ${ tableName }:`, response.Items );

        return response.Items ?? [];
    }

    async drop( tableName: string ) {
        const command = new DeleteTableCommand( { TableName: tableName } );

        debug( `Dropping table: ${ tableName }...` );

        const response = await this.client.send( command );

        debug( `Table dropped successfully: ${ tableName }` );

        return response;
    }

    async dropAll( maxChunkSize = 10 ) {
        const results: DeleteTableCommandOutput[] = [];

        const tableNames = await this.list();
        if ( ! tableNames || tableNames.length === 0 ) {
            debug( "No tables found to drop." );
            return;
        }

        for ( let i = 0 ; i < tableNames.length ; i += maxChunkSize ) {
            const chunk = tableNames.slice( i, i + maxChunkSize );
            console.log( `Deleting tables: ${ chunk.join( ", " ) }` );

            const promises = chunk.map( async ( tableName ) => {
                results.push( await this.drop( tableName ) );
            } );

            await Promise.all( promises );
        }

        console.log( "All tables dropped successfully." );

        return results;
    }
}

