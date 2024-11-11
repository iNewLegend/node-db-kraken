import {
    DYNAMODB_SYMBOLS,
    type TDynamoDBDataTypeSymbol
} from './dynamo-db.definitions';
import { DynamoDBObjectParser } from '../utils/dynamo-db-object.parser';

export class DynamoDBTypeFallbackResolver {
    private static readonly TYPE_FLAGS = {
        [DYNAMODB_SYMBOLS.B]: 1 << 0, // 1
        [DYNAMODB_SYMBOLS.BOOL]: 1 << 1, // 2
        [DYNAMODB_SYMBOLS.N]: 1 << 2, // 4
        [DYNAMODB_SYMBOLS.S]: 1 << 3, // 8
        [DYNAMODB_SYMBOLS.M]: 1 << 4, // 16
        [DYNAMODB_SYMBOLS.BS]: 1 << 5, // 32
        [DYNAMODB_SYMBOLS.L]: 1 << 6, // 64
        [DYNAMODB_SYMBOLS.NS]: 1 << 7, // 128
        [DYNAMODB_SYMBOLS.SS]: 1 << 8, // 256
        [DYNAMODB_SYMBOLS.NULL]: 1 << 9 // 512
    };

    private static readonly SYMBOLS_TO_FLAGS = {
        B: DynamoDBTypeFallbackResolver.TYPE_FLAGS[DYNAMODB_SYMBOLS.B],
        BOOL: DynamoDBTypeFallbackResolver.TYPE_FLAGS[DYNAMODB_SYMBOLS.BOOL],
        N: DynamoDBTypeFallbackResolver.TYPE_FLAGS[DYNAMODB_SYMBOLS.N],
        S: DynamoDBTypeFallbackResolver.TYPE_FLAGS[DYNAMODB_SYMBOLS.S],
        M: DynamoDBTypeFallbackResolver.TYPE_FLAGS[DYNAMODB_SYMBOLS.M],
        BS: DynamoDBTypeFallbackResolver.TYPE_FLAGS[DYNAMODB_SYMBOLS.BS],
        L: DynamoDBTypeFallbackResolver.TYPE_FLAGS[DYNAMODB_SYMBOLS.L],
        NS: DynamoDBTypeFallbackResolver.TYPE_FLAGS[DYNAMODB_SYMBOLS.NS],
        SS: DynamoDBTypeFallbackResolver.TYPE_FLAGS[DYNAMODB_SYMBOLS.SS],
        NULL: DynamoDBTypeFallbackResolver.TYPE_FLAGS[DYNAMODB_SYMBOLS.NULL]
    };

    /**
     * Binary mask representing all complex DynamoDB types.
     *
     * COMPLEX_MASK exists because third party software resolves any complex type (BS, L, M, NS, SS) to 'M'.
     *
     * It uses one fast bitwise operation instead of multiple individual checks, making type resolution lightning fast.
     *
     * Usage:
     * @example
     * ```typescript
     * const typeMask = getTypeMask(inputTypes);
     * if (typeMask & COMPLEX_MASK) {
     *     return 'M'; // Complex type detected
     * }
     * ```
     */
    private static readonly COMPLEX_MASK =
        DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.BS |
        DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.L |
        DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.M |
        DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.NS |
        DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.SS;

    // Precomputed type combinations using binary masks.
    private static readonly TYPE_COMBINATIONS: Record<string, number[]> = {
        B: [
            DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.B,
            DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.B |
                DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.NULL
        ],
        BOOL: [
            DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.BOOL,
            DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.BOOL |
                DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.NULL
        ],
        N: [
            DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.N,
            DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.N |
                DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.NULL,
            DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.BOOL |
                DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.N,
            DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.BOOL |
                DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.N |
                DynamoDBTypeFallbackResolver.SYMBOLS_TO_FLAGS.NULL
        ]
    };

    private static getTypeFlag(type: TDynamoDBDataTypeSymbol): number {
        return this.TYPE_FLAGS[type] || 0;
    }

    private static typesToBitMask(types: TDynamoDBDataTypeSymbol[]): number {
        return types.reduce((mask, type) => mask | this.getTypeFlag(type), 0);
    }

    static getFallbackType(
        types: TDynamoDBDataTypeSymbol[]
    ): TDynamoDBDataTypeSymbol {
        const typeMask = this.typesToBitMask(types);

        if (typeMask & this.COMPLEX_MASK) {
            return DYNAMODB_SYMBOLS.M;
        }

        // Check simple type combinations.
        for (const [fallbackType, masks] of Object.entries(
            this.TYPE_COMBINATIONS
        )) {
            if (masks.includes(typeMask)) {
                return DynamoDBObjectParser.getSymbolByKey(fallbackType);
            }
        }

        // Default to S for remaining valid combinations
        return DYNAMODB_SYMBOLS.S;
    }
}
