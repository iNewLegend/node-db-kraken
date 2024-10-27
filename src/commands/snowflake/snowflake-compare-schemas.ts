import * as util from "node:util";
import { SnowflakeClient } from "../../snowflake-db/snowflake-db-client";

interface ICompareSide {
    warehouse: string;
    database: string;
    schema: string;
}

interface ICompareSideConfig extends ICompareSide {
    account: string;
    username: string;
    password: string;
}

async function snowflakeGetSchemaDifferences(
    sourceConfig: ICompareSideConfig,
    targetConfig: ICompareSideConfig
) {
    const sourceClient = new SnowflakeClient(sourceConfig);
    const targetClient = new SnowflakeClient(targetConfig);

    await Promise.all([sourceClient.connect(), targetClient.connect()]);

    function getSchemaQuery( config: ICompareSideConfig ) {
        const schemaQuery = `
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
        return schemaQuery;
    }

    const [sourceSchema, targetSchema] = await Promise.all([
        sourceClient.query(getSchemaQuery( sourceConfig )),
        targetClient.query(getSchemaQuery( targetConfig )),
    ]);

    const differences = {
        missingTables: new Set<string>(),
        missingColumns: new Map<string, string[]>(),
        typeMismatches: new Map<string, { source: string; target: string }>(),
        nullabilityDifferences: new Map<string, { source: string; target: string }>(),
    };

    // Compare schemas
    const sourceTables = new Set<string>(sourceSchema.map((row: any) => row.TABLE_NAME));
    const targetTables = new Set<string>(targetSchema.map((row: any) => row.TABLE_NAME));

    // Find missing tables
    for (const table of sourceTables) {
        if (!targetTables.has(table)) {
            differences.missingTables.add(table);
        }
    }

    // Compare columns and types
    sourceSchema.forEach((sourceCol: any) => {
        const targetCol = targetSchema.find(
            (t: any) => t.TABLE_NAME === sourceCol.TABLE_NAME && t.COLUMN_NAME === sourceCol.COLUMN_NAME
        );

        if (!targetCol) {
            const cols = differences.missingColumns.get(sourceCol.TABLE_NAME) || [];
            cols.push(sourceCol.COLUMN_NAME);
            differences.missingColumns.set(sourceCol.TABLE_NAME, cols);
            return;
        }

        if (sourceCol.DATA_TYPE !== targetCol.DATA_TYPE) {
            differences.typeMismatches.set(`${sourceCol.TABLE_NAME}.${sourceCol.COLUMN_NAME}`, {
                source: sourceCol.DATA_TYPE,
                target: targetCol.DATA_TYPE,
            });
        }

        if (sourceCol.IS_NULLABLE !== targetCol.IS_NULLABLE) {
            differences.nullabilityDifferences.set(`${sourceCol.TABLE_NAME}.${sourceCol.COLUMN_NAME}`, {
                source: sourceCol.IS_NULLABLE,
                target: targetCol.IS_NULLABLE,
            });
        }
    });

    return differences;
}

export async function snowflakeCompareSchemas(commandIndex: number) {
    // Check arguments,
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
        },
        targetConfig = {
            account: process.env.SNOWFLAKE_ACCOUNT!,
            username: process.env.SNOWFLAKE_USERNAME!,
            password: process.env.SNOWFLAKE_PASSWORD!,
            ... target,
        };
    const differences = await snowflakeGetSchemaDifferences( sourceConfig, targetConfig );

    // Add this at the end of the function before returning differences:

    console.log("\nSchema Comparison Analysis:");
    console.log("==========================");

    const formatDifferences = () => {
        const sections = [];

        if (differences.missingTables.size > 0) {
            sections.push(`❌  Missing Tables (${differences.missingTables.size}):\n${Array.from(differences.missingTables).map(t => `    ❌  ${t}`).join('\n')}`);
        }

        if (differences.missingColumns.size > 0) {
            const columnsOutput = Array.from(differences.missingColumns.entries())
                .map(([table, columns]) => `    📋 ${table}: ${columns.join(', ')}`)
                .join('\n');
            sections.push(`📋 Missing Columns (${differences.missingColumns.size} tables):\n${columnsOutput}`);
        }

        if (differences.typeMismatches.size > 0) {
            const typesOutput = Array.from(differences.typeMismatches.entries())
                .map(([column, types]) => `    📊 ${column}: ${types.source} → ${types.target}`)
                .join('\n');
            sections.push(`📊 Type Mismatches (${differences.typeMismatches.size}):\n${typesOutput}`);
        }

        if (differences.nullabilityDifferences.size > 0) {
            const nullabilityOutput = Array.from(differences.nullabilityDifferences.entries())
                .map(([column, nullability]) => `📊 ${column}: ${nullability.source} → ${nullability.target}`)
                .join('\n');
            sections.push(`📊 Nullability Differences (${differences.nullabilityDifferences.size}):\n${nullabilityOutput}`);
        }

        return sections.join('\n\n');
    };

    const totalDifferences = differences.missingTables.size +
        differences.missingColumns.size +
        differences.typeMismatches.size +
        differences.nullabilityDifferences.size;

    if (totalDifferences > 0) {
        console.log(formatDifferences());
        console.log(`\nTotal Differences: ${totalDifferences}`);
    } else {
        console.log("\n✅ Schemas are identical!");
    }
}
