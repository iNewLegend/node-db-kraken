import test from 'node:test';
import assert from 'node:assert';

import { type AttributeValue, DynamoDBClient, ScanCommand } from "@aws-sdk/client-dynamodb";
import * as util from "node:util";

class DynamoDbClientSpec {
    declare dynamoDBClient: DynamoDBClient;

    public async* scanGenerator(
        tableName: string,
        limitPerRequest: number,
        totalItemsCount: number,
        maxParallel: number = 1
    ): AsyncGenerator<Record<string, AttributeValue>[]> {
        const timeStart = performance.now();

        console.log(`Starting scan on table: ${tableName}`);
        console.log(`Parameters - Limit: ${limitPerRequest}, Total Items: ${totalItemsCount}, Parallel Segments: ${maxParallel}`);

        // Use single segment when batch size can handle entire dataset
        const totalSegments = limitPerRequest >= totalItemsCount ? 1 : maxParallel;
        let itemsScanned = 0;
        let lastEvaluatedKeys = new Array(totalSegments).fill(undefined);

        while (itemsScanned < totalItemsCount) {
            let pendingPromises = Array.from({ length: totalSegments }, (_, segment) => {
                const scanParams = {
                    TableName: tableName,
                    Limit: Math.min(limitPerRequest, totalItemsCount - itemsScanned),
                    Segment: segment,
                    TotalSegments: totalSegments,
                    ExclusiveStartKey: lastEvaluatedKeys[segment]
                };
                console.log(`Scanning segment ${segment} with params:`, util.inspect(scanParams, { compact: true, colors: true, breakLength: Infinity }));
                return {
                    segment,
                    promise: this.dynamoDBClient.send(new ScanCommand(scanParams))
                };
            });

            // Process results as they arrive
            while (pendingPromises.length > 0) {
                console.log(`.    Starting to await for ${pendingPromises.length} pending promises`);
                const result = await Promise.race(pendingPromises.map(p => p.promise));
                const completedIndex = pendingPromises.findIndex(async p => await p.promise === result);
                console.log(`.    Finished await ${pendingPromises.length} pending promises completedIndex: ${completedIndex}`);

                const { segment } = pendingPromises[completedIndex];

                if (result.Items?.length) {
                    console.log(`Yielding ${result.Items.length} items from segment ${segment}`);
                    const remainingCount = totalItemsCount - itemsScanned;
                    const itemsToYield = result.Items.slice(0, remainingCount);
                    itemsScanned += itemsToYield.length;
                    lastEvaluatedKeys[segment] = result.LastEvaluatedKey;
                    yield itemsToYield;
                }

                pendingPromises.splice(completedIndex, 1);
            }

            if (!lastEvaluatedKeys.some(key => key !== undefined)) {
                break;
            }
        }

        const timeEnd = performance.now();

        console.log(`Scan completed. Total items scanned: ${itemsScanned} time: ${timeEnd - timeStart}ms`);
    }
}

test('scanGenerator uses single segment for small dataset (limit 1000, count 80)', async (t) => {
    let segmentsUsed = new Set();

    const mockDynamoDBClient = {
        send: async (command: ScanCommand) => {
            const segment = command.input.Segment || 0;
            segmentsUsed.add(segment);

            const items = Array.from({ length: 80 }, (_, i) => ({
                id: { N: i.toString() },
                value: { S: `value-${i}` },
                timestamp: { N: Date.now().toString() }
            }));

            return {
                Items: items,
                Count: items.length,
                ScannedCount: items.length
            };
        }
    };

    const client = new DynamoDbClientSpec();
    client.dynamoDBClient = mockDynamoDBClient as unknown as DynamoDBClient;

    const collectedItems = [];
    for await (const batch of client.scanGenerator('TestTable', 1000, 80 )) {
        collectedItems.push(...batch);
    }

    assert.strictEqual(segmentsUsed.size, 1);
    assert.strictEqual(collectedItems.length, 80);
});

test('scanGenerator handles large datasets with small batch sizes', async (t) => {
    let totalScanned = 0;

    const generateMockItems = (start: number, count: number) => {
        return Array.from({ length: count }, (_, i) => ({
            id: { N: (start + i).toString() },
            value: { S: `value-${start + i}` },
            timestamp: { N: Date.now().toString() }
        }));
    };

    let sendId = 0;
    const mockDynamoDBClient = {
        send: async (command: ScanCommand) => {
            const id = sendId++;
            const segment = command.input.Segment || 0;
            // Random delay between 50-200ms
            const delay = ( segment + 1);

            console.log(`.    ID ${id}-> Segment ${segment} started with ${delay}ms`);

            await new Promise(resolve => setTimeout(resolve, delay));

            console.log(`.    ID ${id}-> Segment ${segment} completed after ${delay}ms`);

            const items = generateMockItems(totalScanned, 5);
            totalScanned += 5;


            return {
                Items: items,
                Count: items.length,
                ScannedCount: items.length,
                LastEvaluatedKey: totalScanned < 4000 ? { id: { N: totalScanned.toString() } } : undefined
            };
        }
    };

    const client = new DynamoDbClientSpec();
    client.dynamoDBClient = mockDynamoDBClient as unknown as DynamoDBClient;

    const collectedItems = [];
    for await (const batch of client.scanGenerator('TestTable', 5, 4000, 3)) {
        collectedItems.push(...batch);
    }

    assert.strictEqual(collectedItems.length, 4000);
    assert.ok(collectedItems.every(item => 'id' in item && 'value' in item && 'timestamp' in item));
    assert.ok(collectedItems.every(item => typeof item.id.N === 'string'));
});
