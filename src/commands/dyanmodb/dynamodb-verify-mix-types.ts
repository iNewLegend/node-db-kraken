import type { DynamoDBClient } from "../../dynamo-db/dynamo-db-client";
import { attributeTypes } from "../../dynamo-db/dynamo-db-defs.ts";

function generateAllPossibleCombinations() {
    const combinations = new Set<string>();

    // Singles
    attributeTypes.forEach( type => {
        combinations.add( type );
    } );

    // Pairs
    for ( let i = 0 ; i < attributeTypes.length ; i++ ) {
        for ( let j = i + 1 ; j < attributeTypes.length ; j++ ) {
            combinations.add( [ attributeTypes[ i ], attributeTypes[ j ] ].join( '-' ) );
        }
    }

    // Triples
    for ( let i = 0 ; i < attributeTypes.length ; i++ ) {
        for ( let j = i + 1 ; j < attributeTypes.length ; j++ ) {
            for ( let k = j + 1 ; k < attributeTypes.length ; k++ ) {
                combinations.add( [ attributeTypes[ i ], attributeTypes[ j ], attributeTypes[ k ] ].join( '-' ) );
            }
        }
    }

    // Quadruples
    for ( let i = 0 ; i < attributeTypes.length ; i++ ) {
        for ( let j = i + 1 ; j < attributeTypes.length ; j++ ) {
            for ( let k = j + 1 ; k < attributeTypes.length ; k++ ) {
                for ( let l = k + 1 ; l < attributeTypes.length ; l++ ) {
                    combinations.add( [ attributeTypes[ i ], attributeTypes[ j ], attributeTypes[ k ], attributeTypes[ l ] ].join( '-' ) );
                }
            }
        }
    }

    return combinations;
}

export async function dynamoDBverifyMixTypes( dbClient: DynamoDBClient ) {
    const expectedCombinations = generateAllPossibleCombinations();
    const actualTables = await dbClient.list();
    const actualCombinations = new Set(
        actualTables
            .filter( t => t.startsWith( 'type-test-' ) )
            .map( t => t.replace( 'type-test-', '' ) )
    );

    console.log( 'Verification Results:' );
    console.log( '--------------------' );
    console.log( `Expected combinations: ${ expectedCombinations.size }` );
    console.log( `Actual combinations: ${ actualCombinations.size }` );

    const expectedCombinationsArray = [ ... expectedCombinations ];
    const actualCombinationsArray = [ ... actualCombinations ];

    const missing = expectedCombinationsArray.filter( c => ! actualCombinations.has( c ) );
    const extra = actualCombinationsArray.filter( c => ! expectedCombinations.has( c ) );

    if ( missing.length ) {
        console.log( '\nMissing combinations:' );
        missing.forEach( c => console.log( c ) );
    }

    if ( extra.length ) {
        console.log( '\nUnexpected combinations:' );
        extra.forEach( c => console.log( c ) );
    }
}
