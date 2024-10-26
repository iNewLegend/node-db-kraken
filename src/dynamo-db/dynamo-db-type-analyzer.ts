import { DynamoDB, type ScanCommandInput } from "@aws-sdk/client-dynamodb";

interface AttributeType {
    [ attribute: string ]: string[];
}

interface AttributeFinalType {
    [ attribute: string ]: string;
}

export class DynamoDBTypeAnalyzer {
    private attributeTypes: AttributeType = {};
    private finalAttributeTypes: AttributeFinalType = {};

    constructor( private dynamoDBClient: DynamoDB ) {
    }

    private getAttributeType( attributeValue: any ): string {
        if ( attributeValue.S !== undefined ) return 'S';
        if ( attributeValue.N !== undefined ) return 'N';
        if ( attributeValue.B !== undefined ) return 'B';
        if ( attributeValue.SS !== undefined ) return 'SS';
        if ( attributeValue.NS !== undefined ) return 'NS';
        if ( attributeValue.BS !== undefined ) return 'BS';
        if ( attributeValue.M !== undefined ) return 'M';
        if ( attributeValue.L !== undefined ) return 'L';
        if ( attributeValue.NULL !== undefined ) return 'NULL';
        if ( attributeValue.BOOL !== undefined ) return 'BOOL';
        throw new Error( `Unknown type for attribute value: ${ attributeValue }` );
    }

    private analyzeItems( items: any[] ) {
        items.forEach( item => {
            Object.keys( item ).forEach( attribute => {
                const type = this.getAttributeType( item[ attribute ] );
                if ( ! this.attributeTypes[ attribute ] ) {
                    this.attributeTypes[ attribute ] = [];
                }
                if ( ! this.attributeTypes[ attribute ].includes( type ) ) {
                    this.attributeTypes[ attribute ].push( type );
                }
            } );
        } );

        this.determineFinalTypes();
    }

    private determineFinalTypes() {
        Object.keys( this.attributeTypes ).forEach( attribute => {
            const types = this.attributeTypes[ attribute ];
            const finalType = this.getFallbackType( types );
            this.finalAttributeTypes[ attribute ] = this.mapToDatabaseType( finalType );
        } );
    }

    getFallbackType( types: string[] ): string {
        switch ( types.join( ',' ) ) {
            case 'B':
            case 'B,NULL':
                return 'B';

            case 'BOOL':
            case "BOOL,NULL":
                return "BOOL";

            case 'N':
            case 'N,BOOL':
            case 'N,BOOL,NULL':
            case 'N,NULL':
                return 'N';

            default:
                return "M";
        }
    }

    async scanTable( tableName: string ) {
        const params: ScanCommandInput = {
            TableName: tableName,
        };

        let items: any[] = [];
        let lastEvaluatedKey;

        do {
            const result = await this.dynamoDBClient.scan( params );

            items = items.concat( result.Items || [] );

            lastEvaluatedKey = result.LastEvaluatedKey;

            params.ExclusiveStartKey = lastEvaluatedKey;
        } while ( lastEvaluatedKey );

        this.analyzeItems( items );
    }

    private mapToDatabaseType( type: string ): string {
        switch ( type ) {
            case 'S':
                return 'STRING';
            case 'N':
                return 'NUMBER';
            case 'B':
                return 'BINARY';
            case 'BOOL':
                return 'BOOLEAN';
            case 'M':
                return 'OBJECT';
            case 'L':
                return 'ARRAY';
            case 'NULL':
                return 'NULL';
            case 'SS':
                return 'STRING_SET';
            case 'NS':
                return 'NUMBER_SET';
            case 'BS':
                return 'BINARY_SET';
            default:
                throw new Error( `Unsupported type: ${ type }` );
        }
    }

    getFinalAttributeTypes(): AttributeFinalType {
        return this.finalAttributeTypes;
    }
}
