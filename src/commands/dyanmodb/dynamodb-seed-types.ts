import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";
import { faker } from "@faker-js/faker";
import { attributeTypes } from "../../dynamo-db/dynamo-db-defs.ts";


function generateValueForType( type: string ) {
    switch ( type ) {
        case 'S':
            return { S: faker.string.sample() };
        case 'N':
            return { N: faker.number.int().toString() };
        case 'B':
            return { B: Buffer.from( faker.string.sample() ).toString( 'base64' ) };
        case 'BOOL':
            return { BOOL: faker.datatype.boolean() };
        case 'NULL':
            return { NULL: true };
        case 'SS':
            return { SS: [ faker.string.sample(), faker.string.sample() ] };
        case 'NS':
            return { NS: [ faker.number.int().toString(), faker.number.int().toString() ] };
        case 'BS':
            return { BS: [ Buffer.from( faker.string.sample() ).toString( 'base64' ) ] };
        case 'L':
            return { L: [ { S: faker.string.sample() } ] };
        case 'M':
            return { M: { nested: { S: faker.string.sample() } } };
        default:
            return { S: faker.string.sample() };
    }
}

function generateCombinations() {
    const combinations: string[][] = [];

    // Single type
    attributeTypes.forEach( type => combinations.push( [ type ] ) );

    // Pairs
    for ( let i = 0 ; i < attributeTypes.length ; i++ ) {
        for ( let j = i + 1 ; j < attributeTypes.length ; j++ ) {
            combinations.push( [ attributeTypes[ i ], attributeTypes[ j ] ] );
        }
    }

    // Triples
    for ( let i = 0 ; i < attributeTypes.length ; i++ ) {
        for ( let j = i + 1 ; j < attributeTypes.length ; j++ ) {
            for ( let k = j + 1 ; k < attributeTypes.length ; k++ ) {
                combinations.push( [ attributeTypes[ i ], attributeTypes[ j ], attributeTypes[ k ] ] );
            }
        }
    }

    // Quadruples
    for ( let i = 0 ; i < attributeTypes.length ; i++ ) {
        for ( let j = i + 1 ; j < attributeTypes.length ; j++ ) {
            for ( let k = j + 1 ; k < attributeTypes.length ; k++ ) {
                for ( let l = k + 1 ; l < attributeTypes.length ; l++ ) {
                    combinations.push( [ attributeTypes[ i ], attributeTypes[ j ], attributeTypes[ k ], attributeTypes[ l ] ] );
                }
            }
        }
    }

    return combinations;
}

function generateItems( types: string[], count: number = 5 ) {
    const items = [];
    for ( let i = 0 ; i < count ; i++ ) {
        const item: any = {
            id: { S: faker.string.uuid() }
        };

        types.forEach( ( type, index ) => {
            item[ `attr_${ type }` ] = generateValueForType( type );
        } );

        items.push( item );
    }
    return items;
}

function chunkArray<T>( array: T[], size: number ): T[][] {
    return Array.from( { length: Math.ceil( array.length / size ) }, ( _, i ) =>
        array.slice( i * size, i * size + size )
    );
}

export async function dynamoDBseedTypes( dbClient: DynamoDBClient ) {
    const combinations = generateCombinations();
    console.log( `Generated ${ combinations.length } type combinations` );

    const chunkedCombinations = chunkArray( combinations, 10 );

    for ( const [ chunkIndex, combinationChunk ] of chunkedCombinations.entries() ) {
        console.log( `Processing chunk ${ chunkIndex + 1 }/${ chunkedCombinations.length }` );

        const tablePromises = combinationChunk.map( async types => {
            const tableName = `type-test-${ types.join( '-' ) }`;

            try {
                console.log( `Creating table ${ tableName }` );
                await dbClient.create( {
                    TableName: tableName,
                    AttributeDefinitions: [
                        { AttributeName: "id", AttributeType: "S" }
                    ],
                    KeySchema: [
                        { AttributeName: "id", KeyType: "HASH" }
                    ],
                    ProvisionedThroughput: {
                        ReadCapacityUnits: 1,
                        WriteCapacityUnits: 1
                    }
                }, true );

                const items = generateItems( types );
                await dbClient.insertChunks( tableName, items, 25 );
                console.log( `Seeded ${ items.length } items into ${ tableName }` );
            } catch ( e ) {
                console.error( `Failed to process table ${ tableName }:`, e );
            }

            // Sleep for 1 second to avoid rate limiting
            await new Promise( resolve => setTimeout( resolve, 200 ) );
        } );

        await Promise.all( tablePromises );
    }

    console.log( 'Type seeding completed' );
}
