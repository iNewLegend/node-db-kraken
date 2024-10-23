import { faker } from "@faker-js/faker";

/**
 * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/HowItWorks.NamingRulesDataTypes.html#HowItWorks.NamingRules
 *
 * Naming rules
 *
 * Tables, attributes, and other objects in DynamoDB must
 * have names. Names should be meaningful and concise—
 * for example, names such as Products, Books, and Authors
 * are self-explanatory.
 *
 * The following are the naming rules for DynamoDB:
 *
 * - All names must be encoded using UTF-8, and are
 * case-sensitive.
 *
 * - Table names and index names must be between 3 and 255
 * characters long, and can contain only the following
 * characters:
 *
 *   - a-z
 *   - A-Z
 *   - 0-9
 *   - _ (underscore)
 *   - - (dash)
 *   - . (dot)
 *
 * - Attribute names must be at least one character long
 * and less than 64 KB in size. It is considered best
 * practice to keep your attribute names as short as
 * possible. This helps reduce read request units
 * consumed, as attribute names are included in metering
 * of storage and throughput usage.
 *
 * The following are the exceptions. These attribute
 * names must be no greater than 255 characters long:
 *
 * - Secondary index partition key names
 * - Secondary index sort key names
 * - The names of any user-specified projected attributes
 * (applicable only to local secondary indexes)
 *
 * Reserved words and special characters
 *
 * DynamoDB has a list of reserved words and special
 * characters. For a complete list:
 * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
 *
 * in DynamoDB. Also, the following characters have
 * special meaning in DynamoDB: # (hash) and : (colon).
 *
 * Although DynamoDB allows you to use these reserved
 * words and special characters for names, we recommend
 * that you avoid doing so because you have to define
 * placeholder variables whenever you use these names in
 * an expression.
 *
 * For more information
 * @see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ExpressionAttributeNames.html
 * attribute names (aliases) in DynamoDB.
 */

/**
 * DynamoDB supports many different data types for attributes within a table. They can be categorized as follows:
 * * **Scalar Types** – A scalar type can represent exactly one value. The scalar types are number, string, binary, Boolean, and null.
 * * **Document Types** – A document type can represent a complex structure with nested attributes, such as what you would find in a JSON document. The document types are list and map.
 * * **Set Types** – A set type can represent multiple scalar values. The set types are string set, number set, and binary set.
 *
 * When you create a table or a secondary index, you must specify the names and data types of each primary key attribute (partition key and sort key). Furthermore, each primary key attribute must be defined as type string, number, or binary.
 *
 * DynamoDB is a NoSQL database and is _schemaless_. This means that other than the primary key attributes, you don't have to define any attributes or data types when you create tables. By comparison, relational databases require you to define the names and data types of each column when you create a table.
 *
 * The following are descriptions of each data type, along with examples in JSON format.
 *
 * ### Scalar types
 * The scalar types are number, string, binary, Boolean, and null.
 *
 * #### Number
 * Numbers can be positive, negative, or zero. Numbers can have up to 38 digits of precision. Exceeding this results in an exception. If you need greater precision than 38 digits, you can use strings.
 * * Positive range: 1E-130 to 9.9999999999999999999999999999999999999E+125
 * * Negative range: -9.9999999999999999999999999999999999999E+125 to -1E-130
 *
 * In DynamoDB, numbers are represented as variable length. Leading and trailing zeroes are trimmed.
 *
 * All numbers are sent across the network to DynamoDB as strings to maximize compatibility across languages and libraries. However, DynamoDB treats them as number type attributes for mathematical operations.
 *
 * You can use the number data type to represent a date or a timestamp. One way to do this is by using epoch time—the number of seconds since 00:00:00 UTC on 1 January 1970. For example, the epoch time `1437136300` represents 12:31:40 PM UTC on 17 July 2015.
 *
 * For more information, see [http://en.wikipedia.org/wiki/Unix\_time](http://en.wikipedia.org/wiki/Unix_time).
 *
 * #### String
 * Strings are Unicode with UTF-8 binary encoding. The minimum length of a string can be zero, if the attribute is not used as a key for an index or table, and is constrained by the maximum DynamoDB item size limit of 400 KB.
 *
 * The following additional constraints apply to primary key attributes that are defined as type string:
 * * For a simple primary key, the maximum length of the first attribute value (the partition key) is 2048 bytes.
 * * For a composite primary key, the maximum length of the second attribute value (the sort key) is 1024 bytes.
 *
 * DynamoDB collates and compares strings using the bytes of the underlying UTF-8 string encoding. For example, "`a`" (0x61) is greater than "`A`" (0x41), and "`¿`" (0xC2BF) is greater than "`z`" (0x7A).
 *
 * You can use the string data type to represent a date or a timestamp. One way to do this is by using ISO 8601 strings, as shown in these examples:
 * * `2016-02-15`
 * * `2015-12-21T17:42:34Z`
 * * `20150311T122706Z`
 *
 * For more information, see [http://en.wikipedia.org/wiki/ISO\_8601](http://en.wikipedia.org/wiki/ISO_8601).
 *
 * ###### Note
 * Unlike conventional relational databases, DynamoDB does not natively support a date and time data type. It can be useful instead to store date and time data as a number data type, using Unix epoch time.
 *
 * #### Binary
 * Binary type attributes can store any binary data, such as compressed text, encrypted data, or images. Whenever DynamoDB compares binary values, it treats each byte of the binary data as unsigned.
 *
 * The length of a binary attribute can be zero, if the attribute is not used as a key for an index or table, and is constrained by the maximum DynamoDB item size limit of 400 KB.
 *
 * If you define a primary key attribute as a binary type attribute, the following additional constraints apply:
 * * For a simple primary key, the maximum length of the first attribute value (the partition key) is 2048 bytes.
 * * For a composite primary key, the maximum length of the second attribute value (the sort key) is 1024 bytes.
 *
 * Your applications must encode binary values in base64-encoded format before sending them to DynamoDB. Upon receipt of these values, DynamoDB decodes the data into an unsigned byte array and uses that as the length of the binary attribute.
 *
 * The following example is a binary attribute, using base64-encoded text.
 * `dGhpcyB0ZXh0IGlzIGJhc2U2NC1lbmNvZGVk`
 *
 * #### Boolean
 * A Boolean type attribute can store either `true` or `false`.
 *
 * #### Null
 * Null represents an attribute with an unknown or undefined state.
 *
 * ### Document types
 * The document types are list and map. These data types can be nested within each other, to represent complex data structures up to 32 levels deep.
 *
 * There is no limit on the number of values in a list or a map, as long as the item containing the values fits within the DynamoDB item size limit (400 KB).
 *
 * An attribute value can be an empty string or empty binary value if the attribute is not used for a table or index key. An attribute value cannot be an empty set (string set, number set, or binary set), however, empty lists and maps are allowed. Empty string and binary values are allowed within lists and maps. For more information, see [Attributes](./ServiceQuotas.html#limits-attributes).
 *
 * #### List
 * A list type attribute can store an ordered collection of values. Lists are enclosed in square brackets: `[ ... ]`
 *
 * A list is similar to a JSON array. There are no restrictions on the data types that can be stored in a list element, and the elements in a list element do not have to be of the same type.
 *
 * The following example shows a list that contains two strings and a number.
 * `FavoriteThings: ["Cookies", "Coffee", 3.14159]`
 *
 * ###### Note
 * DynamoDB lets you work with individual elements within lists, even if those elements are deeply nested. For more information, see [Using expressions in DynamoDB](./Expressions.html).
 *
 * #### Map
 * A map type attribute can store an unordered collection of name-value pairs. Maps are enclosed in curly braces: `{ ... }`
 *
 * A map is similar to a JSON object. There are no restrictions on the data types that can be stored in a map element, and the elements in a map do not have to be of the same type.
 *
 * Maps are ideal for storing JSON documents in DynamoDB. The following example shows a map that contains a string, a number, and a nested list that contains another map.
 * `{ Day: "Monday", UnreadEmails: 42, ItemsOnMyDesk: [ "Coffee Cup", "Telephone", { Pens: { Quantity : 3}, Pencils: { Quantity : 2}, Erasers: { Quantity : 1} ]}`
 *
 * ###### Note
 * DynamoDB lets you work with individual elements within maps, even if those elements are deeply nested. For more information, see [Using expressions in DynamoDB](./Expressions.html).
 *
 * ### Sets
 * DynamoDB supports types that represent sets of number, string, or binary values. All the elements within a set must be of the same type. For example, a Number Set can only contain numbers and a String Set can only contain strings.
 *
 * There is no limit on the number of values in a set, as long as the item containing the values fits within the DynamoDB item size limit (400 KB).
 *
 * Each value within a set must be unique. The order of the values within a set is not preserved. Therefore, your applications must not rely on any particular order of elements within the set. DynamoDB does not support empty sets, however, empty string and binary values are allowed within a set.
 *
 * The following example shows a string set, a number set, and a binary set:
 * `["Black", "Green", "Red"] [42.2, -19, 7.5, 3.14] ["U3Vubnk=", "UmFpbnk=", "U25vd3k="]`
 *
 * Data type descriptors
 *
 * The low-level DynamoDB API protocol uses _Data type descriptors_ as tokens that tell DynamoDB how to interpret each attribute.
 *
 * The following is a complete list of DynamoDB data type descriptors:
 * * `S` – String
 * * `N` – Number
 * * `B` – Binary
 * * `BOOL` – Boolean
 * * `NULL` – Null
 * * `M` – Map
 * * `L` – List
 * * `SS` – String Set
 * * `NS` – Number Set
 * * `BS` – Binary Set
 */

/**
 * Generates a scalar value of type number for DynamoDB.
 * @returns {number}
 */
const generateNumber = (): number => {
    return faker.number.float( { min: -1e37, max: 1e37, fractionDigits: 10 } );
};

/**
 * Generates a scalar value of type string for DynamoDB.
 * @returns {string}
 */
const generateString = ( min = 1, max = 254 ): string => {
    return faker.string.alphanumeric( { length: { min, max } } );
};

/**
 * Generates a scalar value of type binary for DynamoDB.
 * @returns {string} - base64 encoded binary data
 */
const generateBinary = (): string => {
    const length = faker.number.int( { min: 1, max: 1024 } );
    return Buffer.from( faker.string.alphanumeric( length ) ).toString( 'base64' );
};

/**
 * Generates a scalar value of type Boolean for DynamoDB.
 * @returns {boolean}
 */
const generateBoolean = (): boolean => {
    return faker.datatype.boolean();
};

/**
 * Generates a scalar value of type null for DynamoDB.
 * @returns {boolean}
 */
const generateNull = (): boolean => {
    return true;
};

/**
 * Generates a document type value of a type list for DynamoDB.
 * @returns {Array<any>}
 */
let listDepth = 0;
const generateList = ( maxDepth = 2 ) => {
    ++listDepth;

    const items: any[] = [];

    if ( listDepth >= maxDepth ) {
        --listDepth;
        // Generate non-nested-list
        return [ {
            S: "___MAX_DEPTH_REACHED__"
        } ];
    }

    const itemsLength = faker.number.int( { min: 1, max: 10 } );

    for ( let i = 0 ; i < itemsLength ; i++ ) {
        const generators = getRandomGenerators();

        const listItems: any[] = generators.map( g => g.generate() );

        items.push( ... listItems );
    }

    --listDepth;

    return items;
};

/**
 * Generates a document type value of a type map for DynamoDB.
 * @returns {object}
 */
let mapDepth = 0;
const generateMap = ( maxDepth = 2 ): object => {
    ++mapDepth;

    const object: Record<string, any> = {};

    if ( mapDepth >= maxDepth ) {
        // Generate non-nested-object
        --mapDepth;

        return {
            [ generateAttributeName() ]: {
                S: "___MAX_DEPTH_REACHED__"
            }
        }
    }

    const itemsLength = faker.number.int( { min: 1, max: 10 } );

    for ( let i = 0 ; i < itemsLength ; i++ ) {
        const generators = getRandomGenerators();

        for ( const g of generators ) {
            object[ generateAttributeName() ] = g.generate();
        }
    }

    --mapDepth;

    return object;
};

/**
 * Generates a set type value of a string set for DynamoDB.
 */
const generateStringSet = () => {
    const set = new Set<string>();
    const setSize = faker.number.int( { min: 1, max: 10 } );

    while ( set.size < setSize ) {
        set.add( faker.word.words( { count: { min: 1, max: 10 } } ) );
    }

    return Array.from( set );
};

/**
 * Generates a set type value of number set for DynamoDB.
 */
const generateNumberSet = () => {
    const set = new Set<string>();
    const setSize = faker.number.int( { min: 1, max: 10 } );

    while ( set.size < setSize ) {
        set.add( `${ faker.number.float( { min: -1e37, max: 1e37, fractionDigits: 10 } ) }` );
    }

    return Array.from( set );
};

/**
 * Generates a set type value of a binary set for DynamoDB.
 * TODO: Fix problem with duplication
 */
const generateBinarySet = () => {
    const set = new Set<string>();
    const setSize = faker.number.int( { min: 1, max: 10 } );

    while ( set.size < setSize ) {
        set.add(
            Buffer.from( faker.string.alphanumeric( { length: { min: 1, max: 1024 } } ) )
                .toString( 'base64' )
        );
    }

    return Array.from( set );
};

// valueGenerators object containing all generator functions
const valueGenerators = {
    generateNumber,
    generateString,
    generateBinary,
    generateBoolean,
    generateNull,
    generateList,

    generateMap,
    generateStringSet,
    generateNumberSet,
    // generateBinarySet,
};

/**
 * Generates a DynamoDB compliant table or index name.
 *
 * Table names and index names must be between 3 and 255 characters long, and can contain only the following characters:
 *
 * `a-z`
 * `A-Z`
 * `0-9`
 * `_ (underscore)`
 * `- (dash)`
 * `. (dot)`
 */
function generateTableOrIndexName( minLength = 3, maxLength = 255 ) {
    return faker.helpers.fromRegExp( `[a-zA-Z0-9_.-]{${ minLength },${ maxLength }}` );
}

/**
 * Attribute names must be at least one character long and less than 64 KB in size. It is considered best practice to keep your attribute names as short as possible. This helps reduce read request units consumed, as attribute names are included in metering of storage and throughput usage.
 *
 * The following are the exceptions. These attribute names must be no greater than 255 characters long:
 *
 * Secondary index partition key names
 *
 * Secondary index sort key names
 *
 * The names of any user-specified projected attributes (applicable only to local secondary indexes)
 */
function generateAttributeName() {
    let name = '';

    // Generate a name until it meets the length requirements
    while ( name.length < 1 || name.length > 255 ) {
        // Combine random words
        name = faker.lorem.word();

        // Replace unwanted characters
        name = name.replace( /[^a-zA-Z0-9_.-]/g, '' );
    }

    return name;
}


type TPossibleValueGenerators = keyof typeof valueGenerators;

class DynamoDBSeedGeneratedValue {
    private readonly generator: TPossibleValueGenerators;
    private readonly mark: "N" | "S" | "B" | "BOOL" | "NULL" | "M" | "L" | "SS" | "NS" | "BS";

    private value: any;

    public constructor( generator: TPossibleValueGenerators ) {
        this.generator = generator;

        switch ( generator ) {
            case 'generateNumber':
                this.mark = 'N';
                break;

            case "generateString":
                this.mark = 'S';
                break

            case "generateBinary":
                this.mark = 'B';
                break;

            case "generateBoolean":
                this.mark = 'BOOL';
                break;

            case "generateNull":
                this.mark = 'NULL';
                break;

            case "generateList":
                this.mark = 'L';
                break;

            case "generateMap":
                this.mark = 'M';
                break;

            case "generateStringSet":
                this.mark = 'SS';
                break;

            case "generateNumberSet":
                this.mark = 'NS';
                break;

            case "generateBinarySet":
                this.mark = 'BS';
                break;

            default:
                throw new Error( `Unknown generator: ${ generator }` );

        }
    }

    public generate( withMark = true ) {
        this.value = valueGenerators[ this.generator ]();

        if ( withMark ) {
            return { [ this.mark ]: Number.isInteger( this.value ) ? `${ this.value }` : this.value };
        }
        return this.value;
    }

    public get $() {
        return this.value;
    }

    public getGeneratorName() {
        return this.generator;
    }
}

function getAllGenerators() {
    const keys = Object.keys( valueGenerators ) as TPossibleValueGenerators[];

    return keys.map( key =>
        new DynamoDBSeedGeneratedValue( key )
    );
}

function getRandomGenerators() {
    return faker.helpers.arrayElements( getAllGenerators() );
}

export {
    generateAttributeName,
    generateTableOrIndexName,
    generateNumber,
    generateString,
    generateBinary,
    generateBoolean,
    generateNull,
    generateList,
    generateMap,
    generateStringSet,
    generateNumberSet,
    generateBinarySet,
    getAllGenerators,
    getRandomGenerators,
};
