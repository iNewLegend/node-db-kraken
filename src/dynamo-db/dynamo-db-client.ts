import {
    type AttributeValue,
    CreateTableCommand,
    DeleteTableCommand,
    type DeleteTableCommandOutput,
    DescribeTableCommand,
    DynamoDBClient as DynamoDBClientInternal,
    type DynamoDBClientResolvedConfig,
    GetItemCommand,
    type GlobalSecondaryIndex,
    ListTablesCommand,
    type LocalSecondaryIndex,
    type ProvisionedThroughput,
    PutItemCommand,
    ScanCommand, type ScanCommandInput,
} from "@aws-sdk/client-dynamodb";

import type { TableDescription } from "@aws-sdk/client-dynamodb/dist-types";
import type { ClientInputEndpointParameters } from "@aws-sdk/client-dynamodb/dist-types/endpoint/EndpointParameters";
import type { ListTablesInput } from "@aws-sdk/client-dynamodb/dist-types/models/models_0";

import * as fs from 'fs/promises';
import * as util from "node:util";
import type { TDynamoDBSchema } from "./dynamo-db-types";

util.inspect.defaultOptions.depth = 10;

const debug = util.debug( 'dynamodb:client' );

export class DynamoDBClient {
    public static local() {
        const client = new DynamoDBClientInternal( {
            credentials: {
                accessKeyId: "fakeMyKeyId",
                secretAccessKey: "fakeSecretAccessKey"
            },
            region: "fakeRegion",
            endpoint: "http://localhost:8000",
        } );

        return new DynamoDBClient( client );
    }

    public static awsWithCredentials(
        region: ClientInputEndpointParameters["region"],
        credentials: {
            accessKeyId: string,
            secretAccessKey: string,
        }
    ) {
        const client = new DynamoDBClientInternal( {
            credentials,
            region
        } );

        return new DynamoDBClient( client );
    }

    protected constructor( private client: DynamoDBClientInternal ) {
    }

    public getResolvedConfig(): DynamoDBClientResolvedConfig {
        return this.client.config;
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

    private convertToUint8Array( data: any ): Uint8Array {
        return new Uint8Array( Object.values( data ) );
    }

    // Deep clone function with binary attribute conversion
    private deepCloneWithBinaryConversion( obj: any ): any {
        if ( typeof obj !== 'object' || obj === null ) {
            return obj; // Return the value if obj is not an object
        }

        if ( Array.isArray( obj ) ) {
            return obj.map( item => this.deepCloneWithBinaryConversion( item ) );
        }

        const clone: any = {};

        for ( const key in obj ) {
            if ( obj.hasOwnProperty( key ) ) {
                if ( key === 'B' ) {
                    clone[ key ] = this.convertToUint8Array( obj[ key ] );
                } else if ( key === 'BS' ) {
                    clone[ key ] = obj[ key ].map( ( b: any ) => this.convertToUint8Array( b ) );
                } else if ( typeof obj[ key ] === 'object' ) {
                    clone[ key ] = this.deepCloneWithBinaryConversion( obj[ key ] );
                } else {
                    clone[ key ] = obj[ key ];
                }
            }
        }

        return clone;
    }

    private async loadTablesJSON( filePath: string ) {
        const fileContent = await fs.readFile( filePath, 'utf-8' );
        return JSON.parse( fileContent );
    }

    private async saveTablesJSON( tables: TableDescription[], filePath: string ) {
        debug( "Saving tables to file:", filePath );

        await fs.writeFile( filePath, JSON.stringify( tables, null, 2 ) );

        debug( "Tables saved successfully." );
    }

    public async getTableMetrics( tableName: string, strict = true ) {
        const tableDescription = await this.describe( tableName );

        if ( ! tableDescription ) {
            throw new Error( `Table ${ tableName } not found.` );
        }

        const {
            TableSizeBytes = 0,
            ItemCount = 0,
            partitionKey
        } = tableDescription;

        if ( strict && ( ! TableSizeBytes || ! ItemCount || ! partitionKey ) ) {
            throw new Error(
                `Table ${ tableName } does not have required metrics.`
            );
        }

        return {
            tableSizeBytes: TableSizeBytes,
            itemCount: ItemCount,
            partitionKey
        };
    }

    async list( input: ListTablesInput = {} ) {
        const command = new ListTablesCommand( input );

        debug( "Listing tables...", input );

        const response = await this.client.send( command );

        debug( "Tables found:", response.TableNames );

        return response.TableNames;
    }

    public async describe( tableName: string ): Promise<TDynamoDBSchema> {
        const command = new DescribeTableCommand( { TableName: tableName } );

        debug( `Getting schema for table: ${ tableName }` );

        const { Table } = await this.client.send( command );

        const partitionKey =
            Table?.KeySchema?.find( ( key ) => key.KeyType === 'HASH' )
                ?.AttributeName ?? '';

        debug( `Partition key for table ${ tableName }: ${ partitionKey }` );

        return {
            ... Table,
            partitionKey
        };
    }

    public async get( tableName: string, key: Record<string, AttributeValue> ) {
        const command = new GetItemCommand( {
            TableName: tableName,
            Key: key
        } );

        debug( `Getting item for table: ${ tableName } with key:`, key );

        const response = await this.client.send( command );

        debug( `Item fetched for table ${ tableName }:`, response.Item );

        return response.Item;
    }

    async insert( tableName: string, items: any[] ) {
        for ( const item of items ) {
            try {
                const convertedItem = this.deepCloneWithBinaryConversion( item );

                debug( `Inserting item into ${ tableName }` );

                const command = new PutItemCommand( { TableName: tableName, Item: convertedItem } );

                const result = await this.client.send( command );

                debug( `Inserted item into ${ tableName }: `, result );
            } catch ( error ) {
                console.error( `Failed to insert items into ${ tableName }`, error );
            }
        }
    }

    async getItemById( tableName: string, id: string ) {
        const command = new GetItemCommand( { TableName: tableName, Key: { id: { S: id } } } );

        debug( `Getting item by id: ${ id } from table: ${ tableName }` );

        const { Item } = await this.client.send( command );

        debug( `Item by id ${ id } fetched from table ${ tableName }:`, Item );

        return Item;
    }

    async getSchema( tableName: string ) {
        const command = new DescribeTableCommand( { TableName: tableName } );

        debug( `Getting schema for table: ${ tableName }` );

        const { Table } = await this.client.send( command );
        const partitionKey = Table?.KeySchema?.find( key => key.KeyType === "HASH" )?.AttributeName ?? "";

        debug( `Partition key for table ${ tableName }: ${ partitionKey }` );

        return partitionKey;
    }

    async insertChunks( tableName: string, items: any[], maxChunkSize = 1000 ) {
        const convertedItems = items.map( item => this.deepCloneWithBinaryConversion( item ) );

        debug( `Inserting batch of items into ${ tableName } items count:`, convertedItems.length );

        // Part the items into batches of maxBatchSize
        const batches = [];

        for ( let i = 0 ; i < convertedItems.length ; i += maxChunkSize ) {
            batches.push( convertedItems.slice( i, i + maxChunkSize ) );
        }

        // Send batches of items in parallel
        const promises = batches.map( batch => this.insert( tableName, batch ) );

        await Promise.all( promises );

        debug( `Inserted batch of items into ${ tableName }: items count`, convertedItems.length );

        return convertedItems.length;
    }

    public async create( table: TableDescription, ensureTableActive = false, ensureTimeout = 3000 ) {
        debug( "Creating table:", table.TableName );

        const command = new CreateTableCommand( this.convertTableDescriptionToCreateTableInput( table ) );
        const response = await this.client.send( command );

        debug( `Table ${ table.TableName } created successfully.`, response.TableDescription );

        if ( ensureTableActive ) {
            let triedOnce = false;

            async function isTableActive( this: DynamoDBClient, table: TableDescription ) {
                return new Promise( async ( resolve, reject ) => {
                    debug( `Ensuring table ${ table.TableName } is active with timeout ${ ensureTimeout }ms...` );

                    const tableDescription = await this.describe( table.TableName! );
                    const tableStatus = tableDescription?.TableStatus;

                    if ( tableStatus === "ACTIVE" ) {
                        debug( `Table ${ table.TableName } is active.` );
                        resolve( tableDescription );
                    }

                    if ( triedOnce ) {
                        debug( `Table ${ table.TableName } is not active. Giving up.` );
                        reject( `Table ${ table.TableName } is not active. Giving up.` );
                        return;
                    }

                    if ( tableStatus === "CREATING" ) {
                        debug( `Table ${ table.TableName } is still creating...` );
                        setTimeout( () => resolve( isTableActive.call( this, table ) ), ensureTimeout );
                        return;
                    }
                } )
            }

            await isTableActive.call( this, table );
        }
    }

    public async scan( tableName: string ) {
        const command = new ScanCommand( { TableName: tableName } );

        debug( `Fetching data for table: ${ tableName }` );

        const response = await this.client.send( command );

        debug( `Data fetched for table ${ tableName }:`, response.Items );

        return response.Items ?? [];
    }

    public async* scanGenerator(
        tableName: string,
        maxChunkSize = 10
    ): AsyncGenerator<Record<string, AttributeValue>[]> {
        const items: Record<string, AttributeValue>[] = [];

        let chunkCount = 0;

        let lastEvaluatedKey: Record<string, AttributeValue> | undefined =
            undefined;

        const input: ScanCommandInput = {
            TableName: tableName,
            Limit: maxChunkSize
        };

        do {
            if ( lastEvaluatedKey ) {
                input.ExclusiveStartKey = lastEvaluatedKey;
            }

            const command = new ScanCommand( input );

            debug(
                `Fetching data for table: ${ tableName } with limit: ${ maxChunkSize } chunk: ${ chunkCount }`
            );

            const response = await this.client.send( command );

            debug( `Data fetched for table ${ tableName } chunk:`, chunkCount );

            if ( response.Items?.length ) {
                yield response.Items;

                items.concat( response.Items );
            }

            lastEvaluatedKey = response.LastEvaluatedKey;

            ++chunkCount;
        } while ( lastEvaluatedKey );

        debug( `Total data fetched for table ${ tableName }:`, items );

        return items;
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

    async export( filePath: string ) {
        const tableNames = await this.list();

        if ( ! tableNames || tableNames.length === 0 ) {
            debug( "No tables found to list and save." );
            return;
        }

        const tableDetails = await Promise.all( tableNames.map( async ( tableName ) => {
            const description = await this.describe( tableName );
            const data = await this.scan( tableName );

            if ( ! description || ! data ) {
                throw new Error( `Failed to get table description or data for table ${ tableName }` );
            }

            return {
                ... description,
                data
            };
        } ) );

        debug( "Saving table descriptions and data to file:", filePath );

        await this.saveTablesJSON( tableDetails, filePath );

        debug( "Tables with data saved successfully." );
    }

    async importSchema( filePath: string ) {
        const tables = await this.loadTablesJSON( filePath );

        for ( const table of tables ) {
            debug( `Importing schema for table: ${ table.TableName }` );
            await this.create( table, true );
        }
    }

    async import( filePath: string ) {
        const importedTables: string[] = [];

        await this.importSchema( filePath );

        const tables = await this.loadTablesJSON( filePath );

        for ( const table of tables ) {
            if ( table.data && table.data.length > 0 ) {
                debug( `Inserting data into ${ table.TableName }` );

                await this.insert( table.TableName, table.data );

                importedTables.push( table.TableName );

                debug( `Data inserted into ${ table.TableName }` );
            }
        }

        return importedTables;
    }
}

