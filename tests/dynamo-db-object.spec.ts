import { describe, it } from "node:test";
import assert from "assert";

import { DynamoDBObject, symbols } from "../src/dynamo-db/dynamo-db-object";

describe( "DynamoDBObject", () => {
    it( "should process a string attribute", () => {
        const raw = { S: "John" };
        const obj = new DynamoDBObject( raw );

        assert.strictEqual( obj.getValue(), "John" );
        assert.strictEqual( obj.getSymbol(), symbols.S );
    } );

    it( "should process a number attribute", () => {
        const raw = { N: "30" };
        const obj = new DynamoDBObject( raw );

        assert.strictEqual( obj.getValue(), "30" );
        assert.strictEqual( obj.getSymbol(), symbols.N );
    } );

    it( "should process a boolean attribute", () => {
        const raw = { BOOL: true };
        const obj = new DynamoDBObject( raw );

        assert.strictEqual( obj.getValue(), true );
        assert.strictEqual( obj.getSymbol(), symbols.BOOL );
    } );

    it( "should process a null attribute", () => {
        const raw = { NULL: null };
        const obj = new DynamoDBObject( raw );

        assert.strictEqual( obj.getValue(), null );
        assert.strictEqual( obj.getSymbol(), symbols.NULL );
    } );

    it( "should process a map attribute", () => {
        const raw = { M: { name: { S: "John" } } };
        const obj = new DynamoDBObject( raw );

        const child = obj.getChild( "name" )!;

        assert.strictEqual( child.getValue(), "John" );
        assert.strictEqual( child.getSymbol(), symbols.S );
    } );

    it( "should process a list attribute", () => {
        const raw = { L: [ { S: "John" }, { N: "30" } ] };
        const obj = new DynamoDBObject( raw );

        const child = obj.getChild( "0" ),
            child1 = obj.getChild( "1" );

        assert( Array.isArray( obj.getValue() ) );

        assert.strictEqual( child.getValue(), "John" );
        assert.strictEqual( child.getSymbol(), symbols.S );
        assert.strictEqual( child1.getValue(), "30" );
        assert.strictEqual( child1.getSymbol(), symbols.N );
    } );

    it( "should throw error for invalid key", () => {
        const raw = { INVALID_KEY: "value" };

        assert.throws( () => new DynamoDBObject( raw ) );
    } );

    it( "should process a deeply nested structure", () => {
        const raw = {
            M: {
                level1: {
                    M: {
                        level2: {
                            L: [
                                { S: "John" },
                                { N: "30" },
                                { M: { level3: { BOOL: true } } }
                            ]
                        }
                    }
                }
            }
        };
        const obj = new DynamoDBObject( raw );

        // Level 1
        const level1 = obj.getChild( "level1" )!;
        assert.strictEqual( level1.getSymbol(), symbols.M );

        // Level 2
        const level2 = level1.getChild( "level2" )!;
        assert.strictEqual( level2.getSymbol(), symbols.L );

        // Level 3 - List
        const level3List = level2.parse();
        assert( Array.isArray( level3List ) );
        assert.strictEqual( level3List.length, 3 );
        assert.strictEqual( level3List[ 0 ], "John" );
        assert.strictEqual( level3List[ 1 ], 30 );

        // Level 3 - Map
        const level3Map = level2.getChild( "2" )!;
        const level3 = level3Map.getChild( "level3" )!;
        assert.strictEqual( level3.getSymbol(), symbols.BOOL );
        assert.strictEqual( level3.parse(), true );
    } );

    it( "should return a plain nested object without metadata", () => {
        const raw = {
            M: {
                level1: {
                    M: {
                        level2: {
                            L: [
                                { S: "John" },
                                { N: "30" },
                                { M: { level3: { BOOL: true } } },
                                {
                                    L: [ {
                                        M: {
                                            level4: {
                                                L: [
                                                    { S: "John" },
                                                    { N: "30" },
                                                    { M: { level5: { BOOL: true } } }
                                                ]
                                            }
                                        }
                                    } ]
                                }
                            ]
                        }
                    }
                }
            }
        };

        const obj = new DynamoDBObject( raw );
        const parsed = obj.parse();

        const expected = {
            level1: {
                level2: [
                    "John",
                    30,
                    { level3: true }, [ {
                        level4: [
                            "John",
                            30,
                            { level5: true }
                        ]
                    } ]
                ]
            }
        };

        assert.deepStrictEqual( parsed, expected );
    } );

    it( "should respect maxDepth while parsing", () => {
        const raw = {
            M: {
                level1: {
                    M: {
                        level2: {
                            L: [
                                { S: "John" },
                                { N: "30" },
                                { M: { level3: { BOOL: true } } },
                                {
                                    M: {
                                        level4: {
                                            L: [
                                                { S: "Should not" },
                                                { S: "be" },
                                                { S: "parsed" },
                                                { N: "30" },
                                                { M: { level5: { BOOL: true } } },
                                                { L: [ { S: "Should" }, { S: "not" }, { S: "be" }, { S: "parsed" } ] }
                                            ]
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            }
        };

        let obj = new DynamoDBObject( raw, 1 );
        assert.deepStrictEqual( obj.parse(), {
            level1: '{"level2":["John","30",{"level3":true},{"level4":["Should not","be","parsed","30",{"level5":true},["Should","not","be","parsed"]]}]}'
        } );

        obj = new DynamoDBObject( raw, 2 );
        assert.deepStrictEqual( obj.parse(), {
            level1: { level2: '["John","30",{"level3":true},{"level4":["Should not","be","parsed","30",{"level5":true},["Should","not","be","parsed"]]}]' }
        } );

        obj = new DynamoDBObject( raw, 3 );
        assert.deepStrictEqual( obj.parse(), {
            level1: {
                level2: [
                    'John',
                    30,
                    '{"level3":true}',
                    '{"level4":["Should not","be","parsed","30",{"level5":true},["Should","not","be","parsed"]]}'
                ]
            }
        } );
    } );

    it( "should support 'native' unpacked mode", () => {
        const obj = new DynamoDBObject( {
            M: {
                "foo": { N: 1 },
                "bar": { N: 2 },
                "nested": {
                    M: {
                        "baz": { N: 3 }
                    }
                }

            }
        }, 1 );

        assert.deepStrictEqual( obj.parse(), {
            foo: 1,
            bar: 2,
            nested: '{"baz":3}'
        } );
    } )
} )
