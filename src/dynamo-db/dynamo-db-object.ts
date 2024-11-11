import * as util from 'node:util';

import { AttributeValue } from '@aws-sdk/client-dynamodb';

import {
    TDynamoDBAttributeType,
    TDynamoDBDataTypeSymbol,
    DYNAMODB_SYMBOLS
} from './dynamo-db.definitions';

type TRawValue = Record<string, any>;

export abstract class DynamoDBObject {
    protected symbol: TDynamoDBDataTypeSymbol | null = null;

    // Raw value
    protected value: any = null;

    // Objects wrapped by `DynamoDBObject`.
    protected children: Record<string | number, DynamoDBObject> = {};

    // Should skip parsing the rest of the object.
    protected isByPassed = false;

    private readonly currentDepth: number;

    public static getSymbolByKey(key: string): TDynamoDBDataTypeSymbol {
        return DYNAMODB_SYMBOLS[key as keyof typeof DYNAMODB_SYMBOLS];
    }

    public static getSymbolByAttribute(
        attributeValue: AttributeValue
    ): TDynamoDBDataTypeSymbol {
        const dataTypeSymbol = (function () {
            // noinspection LoopStatementThatDoesntLoopJS
            for (const k in attributeValue) return k as TDynamoDBAttributeType;

            throw new Error(
                `Unknown type for attribute value: ${attributeValue}`
            );
        })();

        return this.getSymbolByKey(dataTypeSymbol);
    }

    public static stringify(raw: TRawValue) {
        return JSON.stringify(raw, (_key, value) => {
            if (value && typeof value === 'object') {
                for (const key in value) {
                    if (Object.prototype.hasOwnProperty.call(value, key)) {
                        return value[key];
                    }
                }

                throw new Error('Invalid object');
            }

            return value;
        });
    }

    constructor(
        private readonly raw: TRawValue,
        private readonly maxProcessDepth = 255,
        depth: number = 0
    ) {
        this.currentDepth = depth;

        let key: TDynamoDBDataTypeSymbol | string = '';
        for (key in this.raw) {
            if (Object.prototype.hasOwnProperty.call(this.raw, key)) {
                break;
            }
        }

        this.symbol = DynamoDBObject.getSymbolByKey(key);

        if (this.currentDepth >= maxProcessDepth) {
            // It means that the rest is packed.
            switch (this.symbol) {
                case DYNAMODB_SYMBOLS.M:
                case DYNAMODB_SYMBOLS.L:
                    this.isByPassed = true;
                    return;
            }
        }

        this.raw = Object.freeze(raw);

        this.process(
            this.symbol,
            this.raw[key as keyof typeof DYNAMODB_SYMBOLS]
        );
    }

    private create(
        raw: TRawValue,
        maxDepth: number = this.maxProcessDepth,
        currentDepth: number = this.currentDepth
    ) {
        // Ensure we respect the extender class when creating new instances of `DynamoDBObject`.
        return new (this.constructor as {
            new (
                raw: TRawValue,
                maxDepth: number,
                depth: number
            ): DynamoDBObject;
        })(raw, maxDepth, currentDepth + 1);
    }

    private process(symbol: TDynamoDBDataTypeSymbol, rawValue: any) {
        switch (symbol) {
            case DYNAMODB_SYMBOLS.S:
            case DYNAMODB_SYMBOLS.N:
            case DYNAMODB_SYMBOLS.B:
            case DYNAMODB_SYMBOLS.BOOL:
            case DYNAMODB_SYMBOLS.NULL:
            case DYNAMODB_SYMBOLS.SS:
            case DYNAMODB_SYMBOLS.NS:
            case DYNAMODB_SYMBOLS.BS:
                this.value = rawValue;
                break;
            case DYNAMODB_SYMBOLS.M:
                this.processMap(rawValue);
                break;

            case DYNAMODB_SYMBOLS.L:
                this.processList(rawValue);
                break;

            default:
                throw new Error(
                    `Invalid symbol for value: ${rawValue} orig:\n ${util.inspect(
                        this.raw,
                        {
                            compact: true,
                            depth: this.maxProcessDepth
                        }
                    )}`
                );
        }
    }

    private processMap(rawValue: Record<string, any>) {
        this.value = {};

        for (const key in rawValue) {
            this.children[key] = this.create({ ...rawValue[key] });

            this.value[key] = this.children[key].value;
        }
    }

    private processList(rawValue: any[]) {
        this.value = [];

        rawValue.forEach((item, index) => {
            this.children[index] = this.create(rawValue[index]);

            this.value[index] = this.children[index].value;
        });
    }

    protected abstract parseImpl(): any;

    public parse() {
        if (this.isByPassed) {
            return DynamoDBObject.stringify(this.raw);
        }

        return this.parseImpl();
    }

    public getValue() {
        return this.value;
    }

    public getChild(key: string | number) {
        return this.children[key];
    }

    public getSymbol() {
        return this.symbol;
    }
}
