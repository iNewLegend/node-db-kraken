import type { ConnectionOptions } from "snowflake-sdk";

export interface ICompareSide {
    warehouse: string;
    database: string;
    schema: string;
}

export interface ICompareSideConfig extends ConnectionOptions {
    account: string;
    username: string;
    password: string;
}

export interface ISchemaDifferences {
    missingTables: Set<string>;
    extraTables: Set<string>;
    missingColumns: Map<string, string[]>;
    extraColumns: Map<string, string[]>;
    typeMismatches: Map<string, { source: string; target: string }>;
    nullabilityDifferences: Map<string, { source: string; target: string }>;
}

