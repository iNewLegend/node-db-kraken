import test from 'node:test';
import assert from 'node:assert';

class TypeFallbackResolverV1 {
    static getFallbackType( types: string[] ): string {
        switch ( types.sort().join( "," ) ) {
            case "B":
            case 'B,NULL':
                return "B";

            case "BOOL":
            case "BOOL,NULL":
                return "BOOL";

            case "BOOL,N":
            case "BOOL,N,NULL":
            case "N":
            case "N,NULL":
                return "N";

            case "S":
            case 'B,BOOL':
            case 'B,BOOL,N':
            case 'B,BOOL,N,NULL':
            case 'B,BOOL,N,S':
            case 'B,BOOL,NULL':
            case 'B,BOOL,NULL,S':
            case 'B,BOOL,S':
            case 'B,N':
            case 'B,N,NULL':
            case 'B,N,NULL,S':
            case 'B,N,S':
            case 'B,NULL,S':
            case 'B,S':
            case 'BOOL,N,NULL,S':
            case 'BOOL,N,S':
            case 'BOOL,NULL,S':
            case 'BOOL,S':
            case 'N,NULL,S':
            case 'N,S':
            case 'NULL,S':
                return "S";

            case "B,BOOL,BS":
            case "B,BOOL,BS,L":
            case "B,BOOL,L":
            case "B,BOOL,L,NS":
            case "B,BOOL,M":
            case "B,BOOL,M,NULL":
            case "B,M":
            case "B,M,NULL":
            case "BOOL,BS,NS,SS":
            case "BOOL,NS,SS":
            case "BS":
            case "BS,NS,S,SS":
            case "N,B,BOOL,M":
            case "N,B,BOOL,M,NULL":
            case "N,B,M":
            case "N,B,M,NULL":
            case "N,BOOL,M":
            case "N,BOOL,M,NULL":
            case "N,M":
            case "N,M,NULL":
            case "S,B,BOOL,M":
            case "S,B,BOOL,M,NULL":
            case "S,B,BOOL,N,M":
            case "S,B,M":
            case "S,B,M,NULL":
            case "S,BOOL,M":
            case "S,BOOL,M,NULL":
            case "S,BOOL,N,M":
            case "S,M":
            case "S,M,NULL":
            case "S,N,B,BOOL,M":
            case "S,N,B,M":
            case "S,N,B,M,NULL":
            case "S,N,BOOL,M":
            case "S,N,BOOL,M,NULL":
            case "S,N,M":
            case "S,N,M,NULL":
            case 'B,BOOL,BS,M':
            case 'B,BOOL,BS,N':
            case 'B,BOOL,BS,NS':
            case 'B,BOOL,BS,NULL':
            case 'B,BOOL,BS,S':
            case 'B,BOOL,BS,SS':
            case 'B,BOOL,L,M':
            case 'B,BOOL,L,N':
            case 'B,BOOL,L,NULL':
            case 'B,BOOL,L,S':
            case 'B,BOOL,L,SS':
            case 'B,BOOL,M,N':
            case 'B,BOOL,M,NS':
            case 'B,BOOL,M,S':
            case 'B,BOOL,M,SS':
            case 'B,BOOL,N,M':
            case 'B,BOOL,N,NS':
            case 'B,BOOL,N,NULL,M':
            case 'B,BOOL,N,S,M':
            case 'B,BOOL,N,SS':
            case 'B,BOOL,NS':
            case 'B,BOOL,NS,NULL':
            case 'B,BOOL,NS,S':
            case 'B,BOOL,NS,SS':
            case 'B,BOOL,NULL,M':
            case 'B,BOOL,NULL,S,M':
            case 'B,BOOL,NULL,SS':
            case 'B,BOOL,S,M':
            case 'B,BOOL,S,SS':
            case 'B,BOOL,SS':
            case 'B,BS':
            case 'B,BS,L':
            case 'B,BS,L,M':
            case 'B,BS,L,N':
            case 'B,BS,L,NS':
            case 'B,BS,L,NULL':
            case 'B,BS,L,S':
            case 'B,BS,L,SS':
            case 'B,BS,M':
            case 'B,BS,M,N':
            case 'B,BS,M,NS':
            case 'B,BS,M,NULL':
            case 'B,BS,M,S':
            case 'B,BS,M,SS':
            case 'B,BS,N':
            case 'B,BS,N,NS':
            case 'B,BS,N,NULL':
            case 'B,BS,N,S':
            case 'B,BS,N,SS':
            case 'B,BS,NS':
            case 'B,BS,NS,NULL':
            case 'B,BS,NS,S':
            case 'B,BS,NS,SS':
            case 'B,BS,NULL':
            case 'B,BS,NULL,S':
            case 'B,BS,NULL,SS':
            case 'B,BS,S':
            case 'B,BS,S,SS':
            case 'B,BS,SS':
            case 'B,L':
            case 'B,L,M':
            case 'B,L,M,N':
            case 'B,L,M,NS':
            case 'B,L,M,NULL':
            case 'B,L,M,S':
            case 'B,L,M,SS':
            case 'B,L,N':
            case 'B,L,N,NS':
            case 'B,L,N,NULL':
            case 'B,L,N,S':
            case 'B,L,N,SS':
            case 'B,L,NS':
            case 'B,L,NS,NULL':
            case 'B,L,NS,S':
            case 'B,L,NS,SS':
            case 'B,L,NULL':
            case 'B,L,NULL,S':
            case 'B,L,NULL,SS':
            case 'B,L,S':
            case 'B,L,S,SS':
            case 'B,L,SS':
            case 'B,M,N':
            case 'B,M,N,NS':
            case 'B,M,N,NULL':
            case 'B,M,N,S':
            case 'B,M,N,SS':
            case 'B,M,NS':
            case 'B,M,NS,NULL':
            case 'B,M,NS,S':
            case 'B,M,NS,SS':
            case 'B,M,NULL,S':
            case 'B,M,NULL,SS':
            case 'B,M,S':
            case 'B,M,S,SS':
            case 'B,M,SS':
            case 'B,N,M':
            case 'B,N,NS':
            case 'B,N,NS,NULL':
            case 'B,N,NS,S':
            case 'B,N,NS,SS':
            case 'B,N,NULL,M':
            case 'B,N,NULL,S,M':
            case 'B,N,NULL,SS':
            case 'B,N,S,M':
            case 'B,N,S,SS':
            case 'B,N,SS':
            case 'B,NS':
            case 'B,NS,NULL':
            case 'B,NS,NULL,S':
            case 'B,NS,NULL,SS':
            case 'B,NS,S':
            case 'B,NS,S,SS':
            case 'B,NS,SS':
            case 'B,NULL,S,M':
            case 'B,NULL,S,SS':
            case 'B,NULL,SS':
            case 'B,S,M':
            case 'B,S,SS':
            case 'B,SS':
            case 'BOOL,BS':
            case 'BOOL,BS,L':
            case 'BOOL,BS,L,M':
            case 'BOOL,BS,L,N':
            case 'BOOL,BS,L,NS':
            case 'BOOL,BS,L,NULL':
            case 'BOOL,BS,L,S':
            case 'BOOL,BS,L,SS':
            case 'BOOL,BS,M':
            case 'BOOL,BS,M,N':
            case 'BOOL,BS,M,NS':
            case 'BOOL,BS,M,NULL':
            case 'BOOL,BS,M,S':
            case 'BOOL,BS,M,SS':
            case 'BOOL,BS,N':
            case 'BOOL,BS,N,NS':
            case 'BOOL,BS,N,NULL':
            case 'BOOL,BS,N,S':
            case 'BOOL,BS,N,SS':
            case 'BOOL,BS,NS':
            case 'BOOL,BS,NS,NULL':
            case 'BOOL,BS,NS,S':
            case 'BOOL,BS,NULL':
            case 'BOOL,BS,NULL,S':
            case 'BOOL,BS,NULL,SS':
            case 'BOOL,BS,S':
            case 'BOOL,BS,S,SS':
            case 'BOOL,BS,SS':
            case 'BOOL,L':
            case 'BOOL,L,M':
            case 'BOOL,L,M,N':
            case 'BOOL,L,M,NS':
            case 'BOOL,L,M,NULL':
            case 'BOOL,L,M,S':
            case 'BOOL,L,M,SS':
            case 'BOOL,L,N':
            case 'BOOL,L,N,NS':
            case 'BOOL,L,N,NULL':
            case 'BOOL,L,N,S':
            case 'BOOL,L,N,SS':
            case 'BOOL,L,NS':
            case 'BOOL,L,NS,NULL':
            case 'BOOL,L,NS,S':
            case 'BOOL,L,NS,SS':
            case 'BOOL,L,NULL':
            case 'BOOL,L,NULL,S':
            case 'BOOL,L,NULL,SS':
            case 'BOOL,L,S':
            case 'BOOL,L,S,SS':
            case 'BOOL,L,SS':
            case 'BOOL,M':
            case 'BOOL,M,L,BS':
            case 'BOOL,M,N':
            case 'BOOL,M,N,NS':
            case 'BOOL,M,N,NULL':
            case 'BOOL,M,N,S':
            case 'BOOL,M,N,SS':
            case 'BOOL,M,NS':
            case 'BOOL,M,NS,NULL':
            case 'BOOL,M,NS,S':
            case 'BOOL,M,NS,SS':
            case 'BOOL,M,NULL':
            case 'BOOL,M,NULL,S':
            case 'BOOL,M,NULL,SS':
            case 'BOOL,M,S':
            case 'BOOL,M,S,SS':
            case 'BOOL,M,SS':
            case 'BOOL,N,NS':
            case 'BOOL,N,NS,NULL':
            case 'BOOL,N,NS,S':
            case 'BOOL,N,NS,SS':
            case 'BOOL,N,NULL,S,M':
            case 'BOOL,N,NULL,SS':
            case 'BOOL,N,S,M':
            case 'BOOL,N,S,SS':
            case 'BOOL,N,SS':
            case 'BOOL,NS':
            case 'BOOL,NS,NULL':
            case 'BOOL,NS,NULL,S':
            case 'BOOL,NS,NULL,SS':
            case 'BOOL,NS,S':
            case 'BOOL,NS,S,SS':
            case 'BOOL,NULL,BS':
            case 'BOOL,NULL,S,M':
            case 'BOOL,NULL,S,SS':
            case 'BOOL,NULL,SS':
            case 'BOOL,S,M':
            case 'BOOL,S,SS':
            case 'BOOL,SS':
            case 'BS,L':
            case 'BS,L,M':
            case 'BS,L,M,N':
            case 'BS,L,M,NS':
            case 'BS,L,M,NULL':
            case 'BS,L,M,S':
            case 'BS,L,M,SS':
            case 'BS,L,N':
            case 'BS,L,N,NS':
            case 'BS,L,N,NULL':
            case 'BS,L,N,S':
            case 'BS,L,N,SS':
            case 'BS,L,NS':
            case 'BS,L,NS,NULL':
            case 'BS,L,NS,S':
            case 'BS,L,NS,SS':
            case 'BS,L,NULL':
            case 'BS,L,NULL,S':
            case 'BS,L,NULL,SS':
            case 'BS,L,S':
            case 'BS,L,S,SS':
            case 'BS,L,SS':
            case 'BS,M':
            case 'BS,M,N':
            case 'BS,M,N,NS':
            case 'BS,M,N,NULL':
            case 'BS,M,N,S':
            case 'BS,M,N,SS':
            case 'BS,M,NS':
            case 'BS,M,NS,NULL':
            case 'BS,M,NS,S':
            case 'BS,M,NS,SS':
            case 'BS,M,NULL':
            case 'BS,M,NULL,S':
            case 'BS,M,NULL,SS':
            case 'BS,M,S':
            case 'BS,M,S,SS':
            case 'BS,M,SS':
            case 'BS,N':
            case 'BS,N,NS':
            case 'BS,N,NS,NULL':
            case 'BS,N,NS,S':
            case 'BS,N,NS,SS':
            case 'BS,N,NULL':
            case 'BS,N,NULL,S':
            case 'BS,N,NULL,SS':
            case 'BS,N,S':
            case 'BS,N,S,SS':
            case 'BS,N,SS':
            case 'BS,NS':
            case 'BS,NS,NULL':
            case 'BS,NS,NULL,S':
            case 'BS,NS,NULL,SS':
            case 'BS,NS,S':
            case 'BS,NS,SS':
            case 'BS,NULL':
            case 'BS,NULL,S':
            case 'BS,NULL,S,SS':
            case 'BS,NULL,SS':
            case 'BS,S':
            case 'BS,S,SS':
            case 'BS,SS':
            case 'L':
            case 'L,M':
            case 'L,M,N':
            case 'L,M,N,NS':
            case 'L,M,N,NULL':
            case 'L,M,N,S':
            case 'L,M,N,SS':
            case 'L,M,NS':
            case 'L,M,NS,NULL':
            case 'L,M,NS,S':
            case 'L,M,NS,SS':
            case 'L,M,NULL':
            case 'L,M,NULL,S':
            case 'L,M,NULL,SS':
            case 'L,M,S':
            case 'L,M,S,SS':
            case 'L,M,SS':
            case 'L,N':
            case 'L,N,NS':
            case 'L,N,NS,NULL':
            case 'L,N,NS,S':
            case 'L,N,NS,SS':
            case 'L,N,NULL':
            case 'L,N,NULL,S':
            case 'L,N,NULL,SS':
            case 'L,N,S':
            case 'L,N,S,SS':
            case 'L,N,SS':
            case 'L,NS':
            case 'L,NS,NULL':
            case 'L,NS,NULL,S':
            case 'L,NS,NULL,SS':
            case 'L,NS,S':
            case 'L,NS,S,SS':
            case 'L,NS,SS':
            case 'L,NULL':
            case 'L,NULL,S':
            case 'L,NULL,S,SS':
            case 'L,NULL,SS':
            case 'L,S':
            case 'L,S,SS':
            case 'L,SS':
            case 'M':
            case 'M,N':
            case 'M,N,NS':
            case 'M,N,NS,NULL':
            case 'M,N,NS,S':
            case 'M,N,NS,SS':
            case 'M,N,NULL':
            case 'M,N,NULL,S':
            case 'M,N,NULL,SS':
            case 'M,N,S':
            case 'M,N,S,SS':
            case 'M,N,SS':
            case 'M,NS':
            case 'M,NS,NULL':
            case 'M,NS,NULL,S':
            case 'M,NS,NULL,SS':
            case 'M,NS,S':
            case 'M,NS,S,SS':
            case 'M,NS,SS':
            case 'M,NULL':
            case 'M,NULL,S':
            case 'M,NULL,S,SS':
            case 'M,NULL,SS':
            case 'M,S':
            case 'M,S,SS':
            case 'M,SS':
            case 'N,NS':
            case 'N,NS,NULL':
            case 'N,NS,NULL,S':
            case 'N,NS,NULL,SS':
            case 'N,NS,S':
            case 'N,NS,S,SS':
            case 'N,NS,SS':
            case 'N,NULL,S,M':
            case 'N,NULL,S,SS':
            case 'N,NULL,SS':
            case 'N,S,M':
            case 'N,S,SS':
            case 'N,SS':
            case 'NS':
            case 'NS,NULL':
            case 'NS,NULL,S':
            case 'NS,NULL,S,SS':
            case 'NS,NULL,SS':
            case 'NS,S':
            case 'NS,S,SS':
            case 'NS,SS':
            case 'NULL,S,M':
            case 'NULL,S,SS':
            case 'NULL,SS':
            case 'S,SS':
            case 'SS':
                return "M";
            default:
                return "M";
        }
    }
}

class TypeFallbackResolverV2 {
    private static readonly TYPE_MAPPINGS: Record<string, Set<string>> = {
        B: new Set( [ "B", "B,NULL" ] ),
        BOOL: new Set( [ "BOOL", "BOOL,NULL" ] ),
        N: new Set( [ "N", "N,NULL", "BOOL,N", "BOOL,N,NULL" ] ),
        S: new Set( [
            "S",
            "B,S",
            "BOOL,S",
            "N,S",
            "NULL,S",
            "B,BOOL,S",
            "B,N,S",
            "BOOL,N,S",
            "B,NULL,S",
            "BOOL,NULL,S",
            "N,NULL,S",
            "B,BOOL",
            "B,BOOL,NULL",
            "N,B",
            "B,N",
            "B,N,NULL,S",
            "B,BOOL,N,S",
            "BOOL,N,NULL,S",
            "B,BOOL,NULL,S",
            "B,N,NULL",
            "B,BOOL,N,NULL",
            "N,B,BOOL",
            "B,BOOL,N",
            "N,B,BOOL,NULL",
        ] ),
    };

    private static readonly COMPLEX_TYPES = new Set( [
        'BS', 'L', 'M', 'NS', 'SS'
    ] );

    private static normalizeTypes( types: string[] ): string {
        return [ ... new Set( types ) ].sort().join( ',' );
    }

    private static hasComplexType( normalizedTypes: string ): boolean {
        return normalizedTypes.split( ',' ).some( type =>
            TypeFallbackResolverV2.COMPLEX_TYPES.has( type )
        );
    }

    static getFallbackType( types: string[] ): string {
        const normalizedTypes = this.normalizeTypes( types );

        if ( this.hasComplexType( normalizedTypes ) ) {
            return 'M';
        }

        for ( const [ fallbackType, combinations ] of Object.entries( this.TYPE_MAPPINGS ) ) {
            if ( combinations.has( normalizedTypes ) ) {
                return fallbackType;
            }
        }

        throw new Error( `Unexpected type combination: ${ normalizedTypes }` );
    }
}

class TypeFallbackResolverV3 {
    private static readonly TYPE_FLAGS = {
        B: 1 << 0,      // 1
        BOOL: 1 << 1,   // 2
        N: 1 << 2,      // 4
        S: 1 << 3,      // 8
        M: 1 << 4,      // 16
        BS: 1 << 5,     // 32
        L: 1 << 6,      // 64
        NS: 1 << 7,     // 128
        SS: 1 << 8,     // 256
        NULL: 1 << 9    // 512
    } as const;

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
        TypeFallbackResolverV3.TYPE_FLAGS.BS |
        TypeFallbackResolverV3.TYPE_FLAGS.L |
        TypeFallbackResolverV3.TYPE_FLAGS.M |
        TypeFallbackResolverV3.TYPE_FLAGS.NS |
        TypeFallbackResolverV3.TYPE_FLAGS.SS;

    // Precomputed type combinations using binary masks.
    private static readonly TYPE_COMBINATIONS: Record<string, number[]> = {
        'B': [
            TypeFallbackResolverV3.TYPE_FLAGS.B,
            TypeFallbackResolverV3.TYPE_FLAGS.B | TypeFallbackResolverV3.TYPE_FLAGS.NULL
        ],
        'BOOL': [
            TypeFallbackResolverV3.TYPE_FLAGS.BOOL,
            TypeFallbackResolverV3.TYPE_FLAGS.BOOL | TypeFallbackResolverV3.TYPE_FLAGS.NULL
        ],
        'N': [
            TypeFallbackResolverV3.TYPE_FLAGS.N,
            TypeFallbackResolverV3.TYPE_FLAGS.N | TypeFallbackResolverV3.TYPE_FLAGS.NULL,
            TypeFallbackResolverV3.TYPE_FLAGS.BOOL | TypeFallbackResolverV3.TYPE_FLAGS.N,
            TypeFallbackResolverV3.TYPE_FLAGS.BOOL | TypeFallbackResolverV3.TYPE_FLAGS.N | TypeFallbackResolverV3.TYPE_FLAGS.NULL
        ]
    };

    private static getTypeFlag( type: string ): number {
        return this.TYPE_FLAGS[ type as keyof typeof TypeFallbackResolverV3.TYPE_FLAGS ] || 0;
    }

    private static typesToBitMask( types: string[] ): number {
        return types.reduce( ( mask, type ) => mask | this.getTypeFlag( type ), 0 );
    }

    static getFallbackType( types: string[] ): string {
        const typeMask = this.typesToBitMask( types );

        if ( typeMask & this.COMPLEX_MASK ) {
            return 'M';
        }

        // Check simple type combinations.
        for ( const [ fallbackType, masks ] of Object.entries( this.TYPE_COMBINATIONS ) ) {
            if ( masks.includes( typeMask ) ) {
                return fallbackType;
            }
        }

        // Default to S for remaining valid combinations
        return 'S';
    }
}

const TEST_CASES = [
    [ 'B', 'NULL' ],
    [ 'BOOL', 'N', 'NULL' ],
    [ 'BS', 'N' ],
    [ 'B', 'BOOL', 'S' ],
    [ 'L', 'M', 'NS' ],
    [ 'B', 'BOOL', 'N', 'NULL' ],
    [ 'BS', 'L', 'M', 'NS', 'SS' ],
    [ 'B', 'BOOL', 'N', 'S', 'NULL', 'BS', 'L', 'M', 'NS', 'SS' ]
];

test( 'Type Resolver Performance Suite', async ( t ) => {
    const iterations = 1000000;

    await t.test( 'warm-up phase', () => {
        TEST_CASES.forEach( testCase => {
            TypeFallbackResolverV1.getFallbackType( testCase );
            TypeFallbackResolverV2.getFallbackType( testCase );
            TypeFallbackResolverV3.getFallbackType( testCase );
        } );
    } );

    await t.test( 'performance comparison', () => {
        const measureImplementation = ( name: string, implementation: any ) => {
            const start = performance.now();
            for ( let i = 0 ; i < iterations ; i++ ) {
                const testCase = TEST_CASES[ i % TEST_CASES.length ];
                implementation.getFallbackType( testCase );
            }
            return performance.now() - start;
        };

        const v1Time = measureImplementation( 'V1', TypeFallbackResolverV1 );
        const v2Time = measureImplementation( 'V2', TypeFallbackResolverV2 );
        const v3Time = measureImplementation( 'V3', TypeFallbackResolverV3 );

        console.table( {
            'V1 Implementation': { time: `${ v1Time.toFixed( 2 ) }ms` },
            'V2 Implementation': { time: `${ v2Time.toFixed( 2 ) }ms` },
            'V3 Implementation': { time: `${ v3Time.toFixed( 2 ) }ms` }
        } );

        // V3 should be less than 200 ms
        assert.strictEqual( v3Time < 200, true, `V3 took more than 200ms` );
    } );

    await t.test( 'result validation', () => {
        TEST_CASES.forEach( testCase => {
            const v1Result = TypeFallbackResolverV1.getFallbackType( testCase );
            const v2Result = TypeFallbackResolverV2.getFallbackType( testCase );
            const v3Result = TypeFallbackResolverV3.getFallbackType( testCase );

            assert.strictEqual( v1Result, v2Result, `V1 and V2 results differ for ${ testCase }` );
            assert.strictEqual( v1Result, v3Result, `V1 and V3 results differ for ${ testCase }` );
        } );
    } );
} );
