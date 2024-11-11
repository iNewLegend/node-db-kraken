import path from 'path';

import { promises as fs } from 'fs';

import * as fsSync from 'fs';
import { DebugLogger } from '../../common/debug-logger';
import type { ICacheStrategy, IScanParallelResult, IStagedCacheData } from '../dynamo-db-defs';


const debug = DebugLogger.create( 'dynamodb:local-cache-strategy' );

const DYNAMODB_BUFFER_SEGMENT_SIZE = 4;

/**
 * `LocalCacheStrategy` is used in a development environment.
 *
 * Class turned to favor memory over speed.
 */
export class DynamoDBLocalCacheStrategy {
    private readonly cacheDir: string;

    private writePromise: Promise<unknown> | null = null;

    public static create( ... privateKey: string[] ): ICacheStrategy {
        return new DynamoDBLocalCacheStrategy( ... privateKey );
    }

    private constructor( ... privacyKey: string[] ) {
        this.cacheDir = path.join(
            process.cwd(),
            'cache',
            privacyKey.join( '/' ),
            'dynamodb'
        );

        debug( () => [ `Cache directory: ${ this.cacheDir }` ] );

        // If the cache directory does not exist, create it
        if ( ! fsSync.existsSync( this.cacheDir ) ) {
            fsSync.mkdirSync( this.cacheDir, { recursive: true } );
        }
    }

    private getFilePath( storage: string, ext = 'json' ): string {
        return path.join( this.cacheDir, `${ storage }.${ ext }` );
    }

    private async getInternal(
        storage: string
    ): Promise<IStagedCacheData | null> {
        const filePath = this.getFilePath( storage );

        debug( () => [ `Reading cache file: ${ filePath }` ] );

        try {
            const data = JSON.parse( await fs.readFile( filePath, 'utf-8' ) );

            return {
                ... data,
                extract: this.readBinary( storage )
            };
        } catch ( e ) {
            return null;
        }
    }

    private async writeBinary(
        storage: string,
        generator: AsyncGenerator<IScanParallelResult>
    ) {
        const binFilePath = this.getFilePath( storage, 'bin' );
        debug( () => [ `Create writing stream to binary file: ${ binFilePath }` ] );

        let writtenBytes = 0;
        let processedRecords = 0;
        const writeStream = fsSync.createWriteStream( binFilePath );

        const writeWithBackpressure = ( buffer: Buffer ): Promise<void> => {
            return new Promise( ( resolve ) => {
                const canWrite = writeStream.write( buffer );
                if ( ! canWrite ) {
                    writeStream.once( 'drain', resolve );
                } else {
                    resolve();
                }
            } );
        };

        // eslint-disable-next-line no-async-promise-executor
        this.writePromise = new Promise( async ( resolve, reject ) => {
            try {
                for await ( const batch of generator ) {
                    const batchBuffer = Buffer.from( JSON.stringify( batch ) );
                    const lengthBuffer = Buffer.alloc(
                        DYNAMODB_BUFFER_SEGMENT_SIZE
                    );
                    lengthBuffer.writeUInt32LE( batchBuffer.length );

                    await writeWithBackpressure( lengthBuffer );
                    await writeWithBackpressure( batchBuffer );

                    writtenBytes +=
                        DYNAMODB_BUFFER_SEGMENT_SIZE + batchBuffer.length;
                    processedRecords += batch.data?.length || 0;

                    debug( () => [
                        `Processed records: ${ processedRecords } writtenBytes: ${ writtenBytes }`
                    ] );
                }

                await new Promise<void>( ( resolveEnd ) => {
                    writeStream.end( () => {
                        debug( () => [
                            `Write stream completed, total records: ${ processedRecords } writtenBytes: ${ writtenBytes }`
                        ] );
                        resolveEnd();
                    } );
                } );
                resolve( true );
            } catch ( error ) {
                writeStream.destroy();
                reject( error );
            }
        } );

        return this.writePromise;
    }

    private async* readBinary( storage: string ) {
        const binFilePath = this.getFilePath( storage, 'bin' );
        debug( () => [ `Reading binary file: ${ binFilePath }` ] );

        if ( ! fsSync.existsSync( binFilePath ) ) {
            debug( () => [ 'Binary file does not exist' ] );
            return;
        }

        const readStream = fsSync.createReadStream( binFilePath, {
            highWaterMark: 16 * 1024
        } );

        let remainingBytes = 0;
        let readBytes = 0;
        let currentLength = 0;
        let pendingData = '';

        for await ( const chunk of readStream ) {
            // Clean up memory.
            if ( global.gc ) {
                global.gc();
            }

            let position = 0;
            while ( position < chunk.length ) {
                if ( remainingBytes === 0 ) {
                    if (
                        chunk.length - position >=
                        DYNAMODB_BUFFER_SEGMENT_SIZE
                    ) {
                        currentLength = chunk.readUInt32LE( position );
                        position += DYNAMODB_BUFFER_SEGMENT_SIZE;
                        remainingBytes = currentLength;
                    } else {
                        break;
                    }
                }

                readBytes += chunk.length;
                if ( chunk.length - position >= remainingBytes ) {
                    const rawData = chunk
                        .slice( position, position + remainingBytes )
                        .toString();
                    try {
                        const data = JSON.parse( pendingData + rawData );
                        yield data;
                        pendingData = '';
                    } catch ( e ) {
                        debug( () => e );
                        pendingData += rawData;
                    }
                    position += remainingBytes;
                    remainingBytes = 0;
                } else {
                    pendingData += chunk.slice( position ).toString();
                    remainingBytes -= chunk.length - position;
                    break;
                }
            }
        }

        debug( () => [ 'Read stream completed, readBytes: ', readBytes ] );
        readStream.close();
    }

    async get( storage: string ) {
        if ( this.writePromise ) {
            await this.writePromise;
        }

        return this.getInternal( storage );
    }

    async set(
        storage: string,
        value: IStagedCacheData,
        generator: AsyncGenerator<IScanParallelResult>
    ): Promise<void> {
        await this.clear( storage );
        const filePath = this.getFilePath( storage );

        return this.writeBinary( storage, generator )
            .then( () => fs.writeFile( filePath, JSON.stringify( value, null, 2 ) ) )
            .then( () => {
                debug( () => [ `Writing cache file: ${ filePath }` ] );
            } )
            .catch( ( e ) => {
                debug( () => [ `Error writing binary data: ${ e }` ] );
            } );
    }

    async clear( storage: string ): Promise<void> {
        const filePath = this.getFilePath( storage );
        await fs.unlink( filePath ).catch( () => {
        } );

        const filePathBin = this.getFilePath( storage, 'bin' );
        await fs.unlink( filePathBin ).catch( () => {
        } );
    }
}
