import snowflake from 'snowflake-sdk';

export class SnowflakeClient {
    private connection: any;

    constructor( config: {
        account: string;
        username: string;
        password: string;
        warehouse: string;
        database: string;
        schema: string;
    } ) {
        this.connection = snowflake.createConnection( config );
    }

    async connect() {
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

    async query( sql: string ): Promise<any> {
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
}
