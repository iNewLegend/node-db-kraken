import { isTrackingColumn } from "../../common/utils";
import { SnowflakeClient } from "../../snowflake-db/snowflake-db-client";
import type { ICompareSide, ICompareSideConfig, ISchemaDifferences } from "../../snowflake-db/snowflake-db-defs";
import { snowflakeFormatTypeWithLength } from "../../snowflake-db/snowflake-db-utils";

async function snowflakeGetSchemaDifferences(
    sourceConfig: ICompareSideConfig,
    targetConfig: ICompareSideConfig
) {
    const sourceClient = new SnowflakeClient( sourceConfig );
    const targetClient = new SnowflakeClient( targetConfig );

    await Promise.all( [ sourceClient.connect(), targetClient.connect() ] );

    function getSchemaQuery( config: ICompareSideConfig ) {
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
        WHERE table_schema = '${ config.schema }'
        AND table_catalog = '${ config.database }'
        ORDER BY table_name, ordinal_position;
    `;
    }

    const sourceSchema = ( await sourceClient.query( getSchemaQuery( sourceConfig ) ) )
        .filter( ( row: any ) => ! isTrackingColumn( row.COLUMN_NAME ) );

    const targetSchema = ( await targetClient.query( getSchemaQuery( targetConfig ) ) )
        .filter( ( row: any ) => ! isTrackingColumn( row.COLUMN_NAME ) );

    const sourceTables = new Set<string>( sourceSchema.map( ( row: any ) => row.TABLE_NAME ) );
    const targetTables = new Set<string>( targetSchema.map( ( row: any ) => row.TABLE_NAME ) );

    const differences: ISchemaDifferences = {
        missingTables: new Set<string>(),
        extraTables: new Set<string>(),
        missingColumns: new Map<string, string[]>(),
        extraColumns: new Map<string, string[]>(),
        typeMismatches: new Map<string, { source: string; target: string }>(),
        nullabilityDifferences: new Map<string, { source: string; target: string }>(),
    };

    // Find missing and extra tables
    for ( const table of sourceTables ) {
        if ( ! targetTables.has( table ) ) {
            differences.missingTables.add( table );
        }
    }

    for ( const table of targetTables ) {
        if ( ! sourceTables.has( table ) ) {
            differences.extraTables.add( table );
        }
    }

    // Find missing and extra columns, type mismatches, and nullability differences
    sourceSchema.forEach( ( sourceCol: any ) => {
        const targetCol = targetSchema.find(
            ( t: any ) => t.TABLE_NAME === sourceCol.TABLE_NAME && t.COLUMN_NAME === sourceCol.COLUMN_NAME
        );

        if ( ! targetCol ) {
            const cols = differences.missingColumns.get( sourceCol.TABLE_NAME ) || [];
            cols.push( sourceCol.COLUMN_NAME );
            differences.missingColumns.set( sourceCol.TABLE_NAME, cols );
            return;
        }

        const sourceType = snowflakeFormatTypeWithLength( sourceCol ),
            targetType = snowflakeFormatTypeWithLength( targetCol );

        if ( sourceType !== targetType ) {
            differences.typeMismatches.set( `${ sourceCol.TABLE_NAME }.${ sourceCol.COLUMN_NAME }`, {
                target: targetType,
                source: sourceType,
            } );
        }

        if ( sourceCol.IS_NULLABLE !== targetCol.IS_NULLABLE ) {
            differences.nullabilityDifferences.set( `${ sourceCol.TABLE_NAME }.${ sourceCol.COLUMN_NAME }`, {
                source: sourceCol.IS_NULLABLE,
                target: targetCol.IS_NULLABLE,
            } );
        }
    } );

    targetSchema.forEach( ( targetCol: any ) => {
        const sourceCol = sourceSchema.find(
            ( s: any ) => s.TABLE_NAME === targetCol.TABLE_NAME && s.COLUMN_NAME === targetCol.COLUMN_NAME
        );

        if ( ! sourceCol ) {
            const cols = differences.extraColumns.get( targetCol.TABLE_NAME ) || [];
            cols.push( targetCol.COLUMN_NAME );
            differences.extraColumns.set( targetCol.TABLE_NAME, cols );
        }
    } );

    return differences;
}

export async function snowflakeCompareSchemas( commandIndex: number ) {
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

    const differences = await snowflakeGetSchemaDifferences( sourceConfig, targetConfig );

    console.log( "\nSchema Comparison Analysis:" );
    console.log( "==========================" );

    const formatDifferences = () => {
        const sections = [];

        if ( differences.missingTables.size > 0 ) {
            sections.push( `❌ Missing in Target (${ differences.missingTables.size }):\n${ Array.from( differences.missingTables ).map( t => `    ❌ ${ t }` ).join( '\n' ) }` );
        }

        if ( differences.extraTables.size > 0 ) {
            sections.push( `➕ Extra in Target (${ differences.extraTables.size }):\n${ Array.from( differences.extraTables ).map( t => `    ➕ ${ t }` ).join( '\n' ) }` );
        }

        if ( differences.missingColumns.size > 0 ) {
            const columnsOutput = Array.from( differences.missingColumns.entries() )
                .map( ( [ table, columns ] ) => `    📋 ${ table }: ${ columns.join( ', ' ) }` )
                .join( '\n' );
            sections.push( `📋 Missing Columns (${ differences.missingColumns.size } tables):\n${ columnsOutput }` );
        }

        if ( differences.extraColumns.size > 0 ) {
            const columnsOutput = Array.from( differences.extraColumns.entries() )
                .map( ( [ table, columns ] ) => `    ➕ ${ table }: ${ columns.join( ', ' ) }` )
                .join( '\n' );
            sections.push( `➕ Extra Columns (${ differences.extraColumns.size } tables):\n${ columnsOutput }` );
        }

        if ( differences.typeMismatches.size > 0 ) {
            const typesOutput = Array.from( differences.typeMismatches.entries() )
                .map( ( [ column, types ] ) => `    📊 ${ column.padEnd( 50, ' ' ) }\t${ types.source }\t→\t${ types.target }` )
                .join( '\n' );
            sections.push( `📊 Type Mismatches (${ differences.typeMismatches.size }):\n${
                '    Name'.padEnd( 50, ' ' ) + "\t\tSource\t\t\tTarget\t"
            }\n${ typesOutput }` );
        }

        if ( differences.nullabilityDifferences.size > 0 ) {
            const nullabilityOutput = Array.from( differences.nullabilityDifferences.entries() )
                .map( ( [ column, nullability ] ) => `    📊 ${ column }: ${ nullability.source } → ${ nullability.target }` )
                .join( '\n' );
            sections.push( `📊 Nullability Differences (${ differences.nullabilityDifferences.size }):\n${ nullabilityOutput }` );
        }

        return sections.join( '\n\n' );
    };

    const totalDifferences = differences.missingTables.size +
        differences.extraTables.size +
        differences.missingColumns.size +
        differences.extraColumns.size +
        differences.typeMismatches.size +
        differences.nullabilityDifferences.size;

    if ( totalDifferences > 0 ) {
        console.log( formatDifferences() );
        console.log( `\nTotal Differences: ${ totalDifferences }` );
    } else {
        console.log( "\n✅ Schemas are identical!" );
    }
}
