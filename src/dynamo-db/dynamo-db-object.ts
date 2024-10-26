import * as util from "node:util";

export const symbols = {
    S: Symbol.for( "S" ),
    N: Symbol.for( "N" ),
    B: Symbol.for( "B" ),
    BOOL: Symbol.for( "BOOL" ),
    NULL: Symbol.for( "NULL" ),
    M: Symbol.for( "M" ),
    L: Symbol.for( "L" ),
    SS: Symbol.for( "SS" ),
    NS: Symbol.for( "NS" ),
    BS: Symbol.for( "BS" ),
}

export type TDynamoDBPossibleSymbols = typeof symbols[keyof typeof symbols];

export class DynamoDBObject {
    protected symbol: TDynamoDBPossibleSymbols | null = null;

    // Raw value
    protected value: any = null;

    // Objects wrapped by `DynamoDBObject`.
    protected children: Record<string | number, DynamoDBObject> = {};

    // Should skip parsing the rest of the object.
    protected isByPassed = false;

    private readonly currentDepth: number;

    public static stringify( raw: Record<string, any> ) {
        return JSON.stringify( raw, ( _key, value ) => {
            if ( value && typeof value === 'object' ) {
                for ( const key in value ) {
                    if ( value.hasOwnProperty( key ) ) {
                        return value[ key ];
                    }
                }

                throw new Error( "Invalid object" );
            }


            return value;
        } );
    }

    public static from( raw: Record<string, any>, depth = 255 ) {
        return new DynamoDBObject( raw, depth ).parse();
    }

    constructor( private readonly raw: Record<string, any>, private readonly maxProcessDepth = 255, depth: number = 0 ) {
        this.currentDepth = depth;

        let key;
        for ( key in this.raw ) {
            if ( this.raw.hasOwnProperty( key ) ) {
                break;
            }
        }

        this.symbol = symbols[ key as keyof typeof symbols ];

        if ( this.currentDepth >= maxProcessDepth ) {
            // It means that the rest is packed.
            switch ( this.symbol ) {
                case symbols.M:
                case symbols.L:
                    this.isByPassed = true;
                    return;
            }
        }

        this.raw = Object.freeze( raw );

        this.process( this.symbol, this.raw[ key as keyof typeof symbols ] );
    }

    private process( symbol: TDynamoDBPossibleSymbols, rawValue: any ) {
        switch ( symbol ) {
            case symbols.S:
            case symbols.N:
            case symbols.B:
            case symbols.BOOL:
            case symbols.NULL:
            case symbols.SS:
            case symbols.NS:
            case symbols.BS:
                this.value = rawValue
                break;
            case symbols.M:
                this.processMap( rawValue );
                break;

            case symbols.L:
                this.processList( rawValue );
                break;

            default:
                throw new Error( `Invalid symbol for value: ${ rawValue } orig:\n ${ util.inspect( this.raw, {
                    compact: true,
                    depth: this.maxProcessDepth
                } ) }` );
        }
    }

    private processMap( rawValue: Record<string, any> ) {
        this.value = {};

        for ( const key in rawValue ) {
            this.children[ key ] = new DynamoDBObject(
                { ... rawValue[ key ] },
                this.maxProcessDepth,
                this.currentDepth + 1
            );

            this.value[ key ] = this.children.value;
        }
    }

    private processList( rawValue: any[] ) {
        this.value = [];

        rawValue.forEach( ( item, index ) => {
            this.children[ index ] = new DynamoDBObject(
                rawValue[ index ],
                this.maxProcessDepth,
                this.currentDepth + 1
            );

            this.value[ index ] = this.children.value;
        } );
    }


    public parse() {
        if ( this.isByPassed ) {
            return DynamoDBObject.stringify( this.raw );
        }

        switch ( this.symbol ) {
            case symbols.S:
                return this.value;

            case symbols.N:
                return Number( this.value );

            case symbols.B:
                return Buffer.from( this.value, "base64" );

            case symbols.BOOL:
                return Boolean( this.value );

            case symbols.NULL:
                return null;

            case symbols.SS:
                return this.value.map( ( item: string ) => item );

            case symbols.NS:
                return this.value.map( ( item: string ) => Number( item ) );

            case symbols.BS:
                return this.value.map( ( item: string ) => Buffer.from( item, "base64" ) );

            case symbols.M:
                const result: any = {};
                this.forEachChildren( ( key: string, child: DynamoDBObject ) => {
                    result[ key ] = child.parse();
                } );
                return result;


            case symbols.L:
                const resultArray: any = [];
                this.forEachChildren( ( key: string, child: DynamoDBObject ) => {
                    resultArray[ key ] = child.parse();
                } );
                return resultArray;

            // Not parsed.
            default:
                return this.value;
        }
    }

    public forEachChildren( callback: ( key: string, value: any ) => void ) {
        return Object.entries( this.children ).forEach(
            ( [ key, value ] ) => callback( key, value )
        )
    }

    public getValue() {
        return this.value;
    }

    public getChild( key: string | number ) {
        return this.children[ key ];
    }

    public getSymbol() {
        return this.symbol;
    }
}
