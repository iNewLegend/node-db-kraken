export class TypeFallbackResolver {
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
        TypeFallbackResolver.TYPE_FLAGS.BS |
        TypeFallbackResolver.TYPE_FLAGS.L |
        TypeFallbackResolver.TYPE_FLAGS.M |
        TypeFallbackResolver.TYPE_FLAGS.NS |
        TypeFallbackResolver.TYPE_FLAGS.SS;

    // Precomputed type combinations using binary masks.
    private static readonly TYPE_COMBINATIONS: Record<string, number[]> = {
        'B': [
            TypeFallbackResolver.TYPE_FLAGS.B,
            TypeFallbackResolver.TYPE_FLAGS.B | TypeFallbackResolver.TYPE_FLAGS.NULL
        ],
        'BOOL': [
            TypeFallbackResolver.TYPE_FLAGS.BOOL,
            TypeFallbackResolver.TYPE_FLAGS.BOOL | TypeFallbackResolver.TYPE_FLAGS.NULL
        ],
        'N': [
            TypeFallbackResolver.TYPE_FLAGS.N,
            TypeFallbackResolver.TYPE_FLAGS.N | TypeFallbackResolver.TYPE_FLAGS.NULL,
            TypeFallbackResolver.TYPE_FLAGS.BOOL | TypeFallbackResolver.TYPE_FLAGS.N,
            TypeFallbackResolver.TYPE_FLAGS.BOOL | TypeFallbackResolver.TYPE_FLAGS.N | TypeFallbackResolver.TYPE_FLAGS.NULL
        ]
    };

    private static getTypeFlag( type: string ): number {
        return this.TYPE_FLAGS[ type as keyof typeof TypeFallbackResolver.TYPE_FLAGS ] || 0;
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
