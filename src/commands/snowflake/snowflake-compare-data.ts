import { diff } from "jest-diff";
import * as util from "node:util";
import { SnowflakeClient } from "../../snowflake-db/snowflake-db-client";
import type { ICompareSide, ICompareSideConfig } from "../../snowflake-db/snowflake-db-defs";

// Set util inspect default color to blue
util.inspect.defaultOptions.colors = true;

Object.keys( util.inspect.styles ).forEach( ( key ) => {
    ( util.inspect.styles as any )[ key ] = "blue";
} );


async function snowflakeCompareTableRows(
    sourceConfig: ICompareSideConfig,
    targetConfig: ICompareSideConfig
) {
    const sourceClient = new SnowflakeClient( sourceConfig );
    const targetClient = new SnowflakeClient( targetConfig );

    await Promise.all( [ sourceClient.connect(), targetClient.connect() ] );

    const sourceTables = new Set<string>(
        ( await sourceClient.snowflakeGetSchema() ).map( ( row: any ) => row.TABLE_NAME )
    );

    for ( const table of sourceTables ) {
        const columnsQuery = `
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '${ sourceConfig.schema }'
              AND table_catalog = '${ sourceConfig.database }'
              AND table_name = '${ table }'
              -- Exclude system columns
              AND column_name NOT LIKE '_%_SYNCED'
              AND column_name NOT LIKE '_%_DELETED'
            ORDER BY ordinal_position
        `;

        const columns = await sourceClient.query( columnsQuery );

        const columnList = columns.map( ( col: any ) => {
            const colName = `"${ col.COLUMN_NAME }"`;
            if ( col.DATA_TYPE.includes( 'TIMESTAMP' ) ) {
                return `TO_VARCHAR(${ colName }) as ${ colName }`;
            }
            return colName;
        } ).join( ', ' );

        const countQuery = `
            SELECT COUNT(*) as diff_count
            FROM ((SELECT ${ columnList }
                   FROM "${ sourceConfig.database }"."${ sourceConfig.schema }"."${ table }")
                  EXCEPT
                  (SELECT ${ columnList }
                   FROM "${ targetConfig.database }"."${ targetConfig.schema }"."${ table }"))
        `;

        const sampleDiffQuery = `
            WITH source_diff AS (SELECT ${ columnList }
                                 FROM "${ sourceConfig.database }"."${ sourceConfig.schema }"."${ table }"
                                 EXCEPT
                                 SELECT ${ columnList }
                                 FROM "${ targetConfig.database }"."${ targetConfig.schema }"."${ table }" LIMIT
                1
                )
               , target_data AS (
            SELECT 'target' as version, ${ columnList }
            FROM "${ targetConfig.database }"."${ targetConfig.schema }"."${ table }"
            WHERE "ID" IN (SELECT "ID" FROM source_diff)
                )
            SELECT 'source' as version, ${ columnList }
            FROM source_diff
            UNION ALL
            SELECT version, ${ columnList }
            FROM target_data
        `;

        const [ diffCount ] = await sourceClient.query( countQuery );

        if ( diffCount.DIFF_COUNT > 0 ) {
            console.log( `\n- ============= 📊 Found differences in table ${ util.inspect( table ) } ============= -` );

            const rows = await sourceClient.query( sampleDiffQuery );
            const sourceRow = rows.find( ( r: { VERSION: string; } ) => r.VERSION === 'source' );
            const targetRow = rows.find( ( r: { VERSION: string; } ) => r.VERSION === 'target' );

            console.log( '\n❗ One item differences sample, total: ' + diffCount.DIFF_COUNT );
            Object.keys( sourceRow ).forEach( key => {
                if ( key !== 'VERSION' && sourceRow[ key ] !== targetRow[ key ] ) {

                    const source = sourceRow[ key ];
                    const target = targetRow[ key ];

                    if ( target.toString() === '[object Object]' && source.toString() === '[object Object]' ) {

                    }

                    let diffOutput = diff( source, target, {
                        aIndicator: '+',
                        bIndicator: '-',
                        contextLines: 1,
                    } );

                    if ( diffOutput && ! diffOutput.includes( 'Compared values have no visual difference' ) ) {
                        diffOutput = diffOutput.split( '\n' ).slice( 3 ).join( '\n' );

                        // Process the lines
                        const lines = diffOutput.split( '\n' ).map( line => line.trim() );

                        const type = lines[ 0 ].replaceAll( ' ', '' )
                            .replace( 'Array[', 'Array' )
                            .replace( 'Object{', 'Object' )

                        // Print the attribute name and Array [ on the same line
                        console.log( `  ${ key }: ${ type }` );

                        // Process the remaining lines with proper indentation
                        const remainingLines = lines.slice( 1, -1 ).map( line =>
                            `     ${ line.replace( '\t', '' ).trim() }`
                        );

                        console.log( remainingLines.join( '\n' ) );

                        return;
                    }

                    console.log( `  ${ key }:` );

                    let a = source.toString();
                    let b = target.toString();

                    if ( a.includes( 'Object' ) && b.includes( 'Object' ) ) {
                        a = util.inspect( source );
                        b = util.inspect( target );
                    } else {
                        a = util.inspect( a );
                        b = util.inspect( b );
                    }

                    console.log( `     Source: ${ a }` );
                    console.log( `     Target: ${ b }` );
                }
            } );
        }
    }
}

export async function snowflakeCompareSchemaData( commandIndex: number ) {
    const [
        sourceWarehouse,
        sourceDatabase,
        sourceSchema,
        targetWarehouse,
        targetDatabase,
        targetSchema,
    ] = process.argv.slice( commandIndex + 1 );

    if ( ! sourceWarehouse || ! sourceDatabase || ! targetWarehouse || ! targetDatabase || ! sourceSchema || ! targetSchema ) {
        console.error(
            "Usage: snowflake-compare-schemas <sourceWarehouse> <sourceDatabase> <sourceSchema> <targetWarehouse> <targetDatabase> <targetSchema>"
        );
        process.exit( 1 );
    }

    const source: ICompareSide = {
        warehouse: sourceWarehouse,
        database: sourceDatabase,
        schema: sourceSchema,
    };

    const target: ICompareSide = {
        warehouse: targetWarehouse,
        database: targetDatabase,
        schema: targetSchema,
    };

    const sourceConfig = {
        account: process.env.SNOWFLAKE_ACCOUNT!,
        username: process.env.SNOWFLAKE_USERNAME!,
        password: process.env.SNOWFLAKE_PASSWORD!,
        ... source,
    };

    const targetConfig = {
        account: process.env.SNOWFLAKE_ACCOUNT!,
        username: process.env.SNOWFLAKE_USERNAME!,
        password: process.env.SNOWFLAKE_PASSWORD!,
        ... target,
    };

    await snowflakeCompareTableRows( sourceConfig, targetConfig );
}

