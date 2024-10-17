import { exec, spawn } from "node:child_process";
import * as fs from "node:fs";
import net from "node:net";
import * as path from "node:path";
import * as crypto from "node:crypto";
import * as util from "node:util";
import * as tar from "tar";
import fetch from "node-fetch";

const DYNAMO_DB_LOCAL_CHECKSUM = 'f5296028d645bb2d3f99fede0a36945956eb7386174430e75c00e6fb1b34e78d';
const NODE_MODULES_DIR = path.dirname( process.env[ "npm_package_json" ]! ) + "/node_modules";
const DYNAMO_DB_LOCAL_DIR = path.join( NODE_MODULES_DIR, ".bin/dynamodb-local" );
const TEMP_TAR_FILE = path.join( "/tmp", "dynamodb_local_latest.tar.gz" );
const DYNAMO_DB_LOCAL_TAR = path.join( DYNAMO_DB_LOCAL_DIR, 'dynamodb_local_latest.tar.gz' );
const DYNAMODB_URL = 'https://d1ni2b6xgvw0s0.cloudfront.net/v2.x/dynamodb_local_latest.tar.gz';

const debug = util.debug( 'dynamodb-local:server' );

function dDBCalculateChecksum( filePath: string ): Promise<string> {
    return new Promise( ( resolve, reject ) => {
        const hash = crypto.createHash( 'sha256' );
        const stream = fs.createReadStream( filePath );

        stream.on( 'data', data => hash.update( data ) );
        stream.on( 'end', () => resolve( hash.digest( 'hex' ) ) );
        stream.on( 'error', reject );
    } );
}

function dDBValidateChecksum( filePath: string, expectedChecksum: string ): Promise<void> {
    return new Promise( async ( resolve, reject ) => {
        try {
            const calculatedChecksum = await dDBCalculateChecksum( filePath );
            if ( calculatedChecksum === expectedChecksum ) {
                resolve();
            } else {
                reject( `Checksum mismatch: expected ${ expectedChecksum }, but got ${ calculatedChecksum }` );
            }
        } catch ( error ) {
            reject( `Error calculating checksum` );
        }
    } );
}

function dDBExtractDynamoDBLocal( file: string ) {
    if ( ! fs.existsSync( DYNAMO_DB_LOCAL_DIR ) ) {
        fs.mkdirSync( DYNAMO_DB_LOCAL_DIR, { recursive: true } );
    }

    return new Promise<void>( ( resolve, reject ) => {
        tar.x( {
            file,
            cwd: DYNAMO_DB_LOCAL_DIR
        }, ( error ) => {
            if ( error ) {
                reject( `Extraction error: ${ error.message }` );
            } else {
                debug( `Extraction completed.` );
                resolve();
            }
        } );
    } );
}

async function dDBDownloadFileWithProgress( url: string, dest: string ): Promise<void> {
    const res = await fetch( url );
    if ( ! res.ok ) {
        throw new Error( `Failed to download DynamoDB Local: ${ res.statusText }` );
    }

    const totalBytes = parseInt( res.headers.get( 'content-length' ) || '0', 10 );
    if ( totalBytes === 0 ) {
        throw new Error( 'Unable to determine the total download size.' );
    }

    const fileStream = fs.createWriteStream( dest );
    let downloadedBytes = 0;

    return new Promise( ( resolve, reject ) => {
        if ( ! res.body ) {
            throw new Error( 'Response body is missing.' );
        }
        res.body.on( 'data', chunk => {
            fileStream.write( chunk );
            downloadedBytes += chunk.length;
            const progress = ( ( downloadedBytes / totalBytes ) * 100 ).toFixed( 2 );
            process.stdout.write( `\rProgress: ${ progress }%` );
        } );

        res.body.on( 'end', () => {
            fileStream.end();
        } );

        fileStream.on( 'finish', () => {
            debug( '\nDownload completed.' );
            resolve();
        } );

        res.body.on( 'error', reject );
        fileStream.on( 'error', reject );
    } );
}

export async function dDBDownload( retryCount = 3 ) {
    try {
        const isTargetEmpty = ( fs.readdirSync( DYNAMO_DB_LOCAL_DIR ).length === 0 );
        if ( isTargetEmpty ) {
            // Remove the LOCAL_DIR if it's empty
            fs.rmdirSync( DYNAMO_DB_LOCAL_DIR, { recursive: true } );
        }
    } catch ( e ) {

    }

    // If file exist in /tmp directory and checksum matches, and the target directory is empty re-extract
    if ( fs.existsSync( TEMP_TAR_FILE ) ) {
        const result = await dDBValidateChecksum( TEMP_TAR_FILE, DYNAMO_DB_LOCAL_CHECKSUM )
            .then( async () => {
                    return "re-extract";
                }, () => {
                    return "re-download"
                }
            ).catch();

        if ( result === "re-download" ) {
            // If checksum doesn't match, delete the file and re-download
            fs.unlinkSync( TEMP_TAR_FILE );

            return dDBDownload( retryCount );
        }

        if ( result === "re-extract" ) {
            return dDBExtractDynamoDBLocal( TEMP_TAR_FILE );
        }

        throw new Error( "Something went wrong" );
    }

    let attempts = 0;
    while ( attempts < retryCount ) {
        try {
            // Ensure the output directory exists and check if DynamoDB Local is already installed
            if ( fs.existsSync( path.join( DYNAMO_DB_LOCAL_DIR ) ) ) {
                return;
            }

            await dDBDownloadFileWithProgress( DYNAMODB_URL, TEMP_TAR_FILE );

            debug( "" )

            await dDBValidateChecksum( TEMP_TAR_FILE, DYNAMO_DB_LOCAL_CHECKSUM );

            await dDBExtractDynamoDBLocal( TEMP_TAR_FILE );
        } catch ( error ) {
            console.error( `Attempt ${ attempts + 1 }/${ retryCount } failed: ${ util.inspect( error, { depth: null } ) }` );
            attempts++;

            if ( attempts >= retryCount ) {
                throw new Error( `Failed to download and install DynamoDB Local after ${ retryCount } attempts.` );
            }
        }
    }
}

export async function dDBLaunch( port = 8000, sharedDb = false, args: string[] = [] ) {
    const DYNAMODB_JAR = path.join( DYNAMO_DB_LOCAL_DIR, 'DynamoDBLocal.jar' );

    // Check if file exists
    if ( ! fs.existsSync( DYNAMODB_JAR ) ) {
        throw new Error( `DynamoDB Local JAR file not found at ${ DYNAMODB_JAR }` );
    }

    const javaArgs = [
        '-Djava.library.path=./DynamoDBLocal_lib',
        '-jar',
        DYNAMODB_JAR,
        '-port',
        port.toString(),
        ... args
    ];

    if ( sharedDb ) {
        javaArgs.push( '-sharedDb' );
    }

    debug( `Launching DynamoDB Local with arguments: ${ javaArgs.join( ' ' ) }` );

    const dynamoDBLocalProcess = spawn( 'java', javaArgs, { cwd: DYNAMO_DB_LOCAL_DIR } );

    dynamoDBLocalProcess.stdout.on( 'data', ( data ) => {
        debug( `DDBServer: ${ data.toString().split( '\n' ).join( '\nDDBServer: ' ) }` );
    } );

    dynamoDBLocalProcess.stderr.on( 'data', ( data ) => {
        console.error( `stderr: ${ data.toString() }` );
    } );

    dynamoDBLocalProcess.on( 'close', ( code ) => {
        if ( code !== 0 ) {
            console.error( `DynamoDB Local process exited with code ${ code }` );
        } else {
            debug( 'DynamoDB Local stopped gracefully' );
        }
    } );

    return dynamoDBLocalProcess;
}

export async function dDBStop( options: { port: number } | { pid: number } ) {
    const { port = 0, pid = 0 } = options as any;

    function killProcessByPID( pid: string ) {
        const killProcessCmd = `kill ${ pid }`;

        exec( killProcessCmd, ( killError ) => {
            if ( killError ) {
                console.error( `Error stopping process with PID ${ pid }:`, killError );
            } else {
                debug( `Successfully stopped ${ port ? `process on port ${ port }` : '' } (PID: ${ pid })` );
            }
        } );
    }

    if ( pid ) {
        killProcessByPID( pid.toString() );
        return;
    }

    const findProcessCmd = `lsof -i :${ port } -t`;

    exec( findProcessCmd, ( error, stdout ) => {
        if ( error ) {
            debug( `Error finding process on port ${ port }:`, error );
            return;
        }

        const pid = stdout.trim();
        if ( ! pid ) {
            debug( `No process found running on port ${ port }` );
            return;
        }
        killProcessByPID( pid );
    } );
}


export async function dDBEnsurePortActivity( port = 8000, retryCount = 3, timeout = 2000 ) {
    async function tryConnect() {
        return await new Promise<Boolean>( ( resolve, reject ) => {
            const socket = new net.Socket();

            socket.connect( 8000, "localhost", () => {
                debug( `Port ${ port } is active` );
                socket.destroy();
                resolve( true );
            } );

            socket.on( "error", reject );

            socket.setTimeout( timeout, () => {
                reject( new Error( `Timeout waiting for port ${ port } to become active` ) );
            } );
        } )
    }

    try {
        await tryConnect()
        return;
    } catch ( e ) {
    }


    for ( let i = 0 ; i < ( retryCount + 1 ) ; i++ ) {
        debug( `Waiting for port ${ port } to become active... Attempt ${ i + 1 } of ${ retryCount }` );

        await new Promise( ( resolve ) => setTimeout( resolve, timeout ) );

        try {
            await tryConnect()
            return;
        } catch ( e ) {

        }
    }

    throw new Error( `Port ${ port } is not active after ${ retryCount } attempts` );
}

let isTerminateProcessing = false;

export function dDBHandleTermination( processPid: number ) {
    async function terminate() {
        if ( isTerminateProcessing ) {
            return;
        }
        isTerminateProcessing = true;
        console.info( "\nShutting down..." );
        await dDBStop( { pid: processPid } );
    }

    process.on( "SIGINT", terminate );
    process.on( "SIGTERM", terminate );

}
