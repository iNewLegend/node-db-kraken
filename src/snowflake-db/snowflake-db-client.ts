import snowflake, { type ConnectionOptions } from 'snowflake-sdk';
import { isTrackingColumn } from '../common/utils';

snowflake.configure( {
    logLevel: 'INFO',
    additionalLogToConsole: false,
    logFilePath: "./logs/snowflake-sdk.log",
} )

export class SnowflakeClient {
    private connection: any;

    constructor( private config: ConnectionOptions ) {
        this.connection = snowflake.createConnection( config );
    }

    private getSchemaQuery() {
        return `
        -- @Language=SnowflakeSQL
        SELECT
            table_name,
            column_name,
            data_type,
            is_nullable,
            character_maximum_length,
            numeric_precision,
            numeric_scale
        FROM information_schema.columns
        WHERE table_schema = '${ this.config.schema }'
        AND table_catalog = '${ this.config.database }'
        ORDER BY table_name, ordinal_position;
    `;
    }

    public async connect() {
        return new Promise( ( resolve, reject ) => {
            this.connection.connect( ( err: any, conn: any ) => {
                if ( err ) {
                    reject( err );
                } else {
                    resolve( conn );
                }
            } );
        } );
    }

    public async query( sql: string ): Promise<any> {
        return new Promise( ( resolve, reject ) => {
            this.connection.execute( {
                sqlText: sql,
                complete: ( err: any, stmt: any, rows: any ) => {
                    if ( err ) {
                        reject( err );
                    } else {
                        resolve( rows );
                    }
                }
            } );
        } );
    }

    public async snowflakeGetSchema( excludeTrackingColumns = true ): Promise<any> {
        const result = ( await this.query( this.getSchemaQuery() ) );

        if ( excludeTrackingColumns ) {
            return result.filter( ( col: any ) => ! isTrackingColumn( col.COLUMN_NAME ) );
        }

        return result;
    }
}
