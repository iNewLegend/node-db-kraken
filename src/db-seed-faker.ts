import { CreateTableCommand, DynamoDBClient, PutItemCommand } from "@aws-sdk/client-dynamodb";
import type { CreateTableCommandInput } from "@aws-sdk/client-dynamodb/dist-types/commands/CreateTableCommand";
import { faker } from "@faker-js/faker";
import { DynamoDBLocalServer } from "./dynamo-db/dynamo-db-server";

const client = new DynamoDBClient( {
    credentials: {
        accessKeyId: "fakeMyKeyId",
        secretAccessKey: "fakeSecretAccessKey"
    },
    region: "fakeRegion",
    endpoint: "http://localhost:8000",
} );

async function lunchDynamoDBLocal() {
    const dynamoDBLocalServer = new DynamoDBLocalServer();

    await dynamoDBLocalServer.downloadInternals();

    const dbProcess = await dynamoDBLocalServer.start();

    console.log( "DynamoDB Local launched with PID:", dbProcess.pid );

    await dynamoDBLocalServer.waitForServerListening();

    return dbProcess;
}

// List of different faker value generators and DynamoDB data types
const valueGenerators = [
    () => ( { S: faker.lorem.word() } ),                      // String
    () => ( { N: faker.number.int().toString() } ),      // Number
    () => ( { N: faker.number.hex().toString() } ),      // Number
    () => ( { N: faker.number.bigInt().toString() } ),      // Number
    () => ( { N: faker.number.octal().toString() } ),      // Number
    () => ( { N: faker.number.float().toString() } ),      // Number
    () => ( { N: faker.number.binary().toString() } ),      // Number
    () => ( { BOOL: faker.datatype.boolean() } ),             // Boolean
    () => ( { S: faker.name.firstName() } ),                  // Name
    () => ( { S: faker.name.lastName() } ),                   // Last Name
    () => ( { S: faker.internet.email() } ),                  // Email
    () => ( { S: faker.phone.number() } ),               // Phone Number
    () => ( { S: faker.address.streetAddress() } ),           // Address
    () => ( { S: faker.address.city() } ),                    // City
    () => ( { S: faker.address.country() } ),                 // Country
    () => ( { S: faker.date.past().toISOString() } ),         // Past Date
    () => ( { S: faker.date.future().toISOString() } ),       // Future Date
    () => ( { S: faker.date.recent().toISOString() } ),       // Recent Date
    () => ( { S: faker.date.soon().toISOString() } ),         // Soon Date
    () => ( { S: faker.word.words() } ),                     // Random Word
    () => ( { S: faker.company.name() } ),             // Company Name
    () => ( { S: faker.commerce.productName() } ),            // Product Name
    () => ( { S: faker.commerce.productAdjective() } ),       // Product Adjective
    () => ( { S: faker.commerce.productMaterial() } ),        // Product Material
    () => ( { S: faker.commerce.product() } ),                // Product
    () => ( { S: faker.commerce.department() } ),             // Department
    () => ( { S: faker.commerce.price().toString() } ),       // Price
    () => ( { S: faker.finance.accountName() } ),             // Account Name
    () => ( { S: faker.finance.accountName() } ),                 // Account
    () => ( { S: faker.finance.amount().toString() } ),       // Amount
    () => ( { S: faker.finance.transactionType() } ),         // Transaction Type
    () => ( { S: faker.hacker.phrase() } ),                   // Hacker Phrase
    () => ( { M: { nestedKey: { S: faker.lorem.sentence() } } } ), // Map
    () => ( { L: [ { S: faker.lorem.word() }, { N: faker.number.int().toString() } ] } ), // List
    () => ( { S: faker.hacker.phrase() } ),                   // Hacker Phrase
    () => ( {
        M: {
            nestedKey: { S: faker.lorem.sentence() }
        }
    } ), // Map
    () => ( {
        L: [
            { S: faker.lorem.word() },
            { N: faker.number.int().toString() }
        ]
    } ) // List

];

const generateRandomKey = (): string => faker.database.column();

const generateRandomValue = () => {
    const randomGenerator = faker.helpers.arrayElement( valueGenerators );
    return randomGenerator();
};

const generateRandomItem = (): Record<string, any> => {
    const item: Record<string, any> = {
        id: { S: faker.string.uuid() } // Ensure there's always an id
    };

    // Randomly generate between 1 and 10 additional attributes
    const attributesCount = faker.number.int( { min: 1, max: 10 } );

    for ( let i = 0 ; i < attributesCount ; i++ ) {
        const key = generateRandomKey();
        const value = generateRandomValue();
        item[ key ] = value;
    }

    return item;
};

const createTable = async ( tableName: string ) => {
    const params: CreateTableCommandInput = {
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
    };

    try {
        await client.send( new CreateTableCommand( params ) );
        console.log( `Created table ${ tableName }` );
    } catch ( err ) {
        console.error( `Failed to create table ${ tableName }:`, err );
    }
};

// Seed a specified number of items into the DynamoDB table
const seedTable = async ( tableName: string, itemCount: number ) => {
    for ( let i = 0 ; i < itemCount ; i++ ) {
        const item = generateRandomItem();
        const params = {
            TableName: tableName,
            Item: item
        };

        try {
            await client.send( new PutItemCommand( params ) );
            console.log( `Added item ${ item.id.S } to ${ tableName }` );
        } catch ( err ) {
            console.error( `Failed to add item ${ item.id.S } to ${ tableName }:`, err );
        }
    }
}

// Main entry point for running the seeding script
const main = async () => {
    await lunchDynamoDBLocal();

    const tablesCount = parseInt( process.argv[ 2 ], 10 ) || 1;
    const itemsCount = parseInt( process.argv[ 3 ], 10 ) || 10;

    for ( let i = 0 ; i < tablesCount ; i++ ) {
        const tableName = faker.database.column() + "_" + faker.string.uuid();
        await createTable( tableName );

        // Wait for the table to be active
        await new Promise( ( resolve ) => setTimeout( resolve, 10000 ) );

        await seedTable( tableName, itemsCount );
    }
    console.log( `Seeding ${ itemsCount } random items into each of ${ tablesCount } tables completed.` );
};

main().catch( ( err ) => {
    console.error( "Error in seeding process:", err );
} );
