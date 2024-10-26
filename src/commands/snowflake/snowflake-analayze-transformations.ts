import process from "node:process";
import { SnowflakeClient } from "../../snowflake-db/snowflake-db-client";

function parseTypeTest(tableName: string): string[] {
    return tableName.replace("TYPE_TEST_", "").split("_").filter(Boolean);
}

function snowflakeTypeToDynamoDBType(snowflakeType: string) {
    switch (snowflakeType) {
        case "NUMBER":
            return "N";

        case "TEXT":
        case "STRING":
            return "S";

        case "BINARY":
            return "B";

        case "BOOLEAN":
            return "BOOL";

        case "VARIANT":
            return "M";

        default:
            throw new Error(`Unknown Snowflake type: ${snowflakeType}`);
    }
}

export async function snowflakeAnalyzeTypeTransformations() {
    const snowflake = new SnowflakeClient({
        account: process.env.SNOWFLAKE_ACCOUNT!,
        username: process.env.SNOWFLAKE_USERNAME!,
        password: process.env.SNOWFLAKE_PASSWORD!,
        warehouse: process.env.SNOWFLAKE_WAREHOUSE!,
        database: process.env.SNOWFLAKE_DATABASE!,
        schema: process.env.SNOWFLAKE_SCHEMA!,
    });

    await snowflake.connect();

    const columnTypesQuery = `
        SELECT DISTINCT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = '${process.env.SNOWFLAKE_SCHEMA}'
          AND table_catalog = '${process.env.SNOWFLAKE_DATABASE}'
          AND table_name LIKE 'TYPE_TEST_%'
          AND column_name LIKE 'ATTR_%'
          AND column_name != 'ID'
          AND column_name != 'DATA'
        ORDER BY table_name;
    `;

    const results = await snowflake.query(columnTypesQuery);
    const transformationMap = new Map<string, string>();

    results.forEach((row: any) => {
        const types = parseTypeTest(row.TABLE_NAME);
        const key = types.join(",");
        transformationMap.set(key, row.DATA_TYPE);
    });

    console.log("\nType Combination Analysis:");
    console.log("------------------------");

    for (const [combination, snowflakeType] of transformationMap) {
        console.log(`${combination} -> ${snowflakeType}`);
    }

    // Ready array data for DynamoDB test eg: `'BOOL,NS': 'NS',`
    const dynamoDbTypes: string[] = [];
    for (const [combination, snowflakeType] of transformationMap) {
        dynamoDbTypes.push(`'${combination}': '${snowflakeTypeToDynamoDBType(snowflakeType)}',`);
    }

    console.log("\nDynamoDB Type Mapping:");
    console.log("------------------------");
    console.log(dynamoDbTypes.join("\n"));

    return transformationMap;
}
