import { exec, spawn } from "node:child_process";
import * as fs from "node:fs";
import net from "node:net";
import * as path from "node:path";

import * as tar from "tar";

const NODE_MODULES_DIR = path.dirname( process.env[ "npm_package_json" ]! ) + "/node_modules";
const DYNAMO_DB_LOCAL_DIR = path.join( NODE_MODULES_DIR, ".bin/dynamodb-local" );
const TEMP_TAR_FILE = path.join( "/tmp", "dynamodb_local_latest.tar.gz" );

const DYNAMODB_URL = 'https://s3.us-west-2.amazonaws.com/dynamodb-local/dynamodb_local_latest.tar.gz';

export async function dDBDownload( retryCount = 3 ) {
    let attempts = 0;
    while ( attempts < retryCount ) {
        try {
            // Ensure the output directory exists and check if DynamoDB Local is already installed
            if ( fs.existsSync( path.join( DYNAMO_DB_LOCAL_DIR ) ) ) {
                return;
            }

            if ( ! fs.existsSync( DYNAMO_DB_LOCAL_DIR ) ) {
                fs.mkdirSync( DYNAMO_DB_LOCAL_DIR, { recursive: true } );
            }
            if ( ! fs.existsSync( DYNAMO_DB_LOCAL_DIR ) ) {
                fs.mkdirSync( DYNAMO_DB_LOCAL_DIR, { recursive: true } );
            }

            // Fetch DynamoDB Local
            const res = await fetch( DYNAMODB_URL );
            if ( ! res.ok ) {
                throw new Error( `Failed to download DynamoDB Local: ${ res.statusText }` );
            }

            if ( ! res.body ) {
                throw new Error( `Failed to download DynamoDB Local: ${ res.statusText }` );
            }

            // Total size of the content (in bytes)
            const totalSize = parseInt( res.headers.get( 'content-length' ) || '0', 10 );
            let receivedSize = 0;

            // Create file stream to write to temporary tar file
            const fileStream = fs.createWriteStream( TEMP_TAR_FILE );

            // Get readable stream from response body
            const reader = res.body.getReader();

            // Read and write data chunks
            while ( true ) {
                const { done, value } = await reader.read();
                if ( done ) break;
                const chunk = Buffer.from( value );

                receivedSize += chunk.length;
                const progress = ( ( receivedSize / totalSize ) * 100 ).toFixed( 2 );
                process.stdout.write( `Downloading DynamoDB Local: ${ progress }%\r` );

                fileStream.write( chunk );
            }

            // Close file stream
            fileStream.end();

            // Extract the tarball
            await tar.x( {
                file: TEMP_TAR_FILE,
                cwd: DYNAMO_DB_LOCAL_DIR,
                strip: 1 // Removes the top-level directory
            } );

            // Clean up the temporary tar file
            fs.unlinkSync( TEMP_TAR_FILE );
            console.log( '\nDynamoDB Local downloaded and extracted successfully.' );
            return;
        } catch ( err ) {
            if ( attempts < retryCount - 1 ) {
                console.log( `Retrying download... Attempt ${ attempts + 2 } of ${ retryCount }` );
            } else {
                throw err;
            }
        }
        attempts++;
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

    console.log( `Launching DynamoDB Local with arguments: ${ javaArgs.join( ' ' ) }` );

    const dynamoDBLocalProcess = spawn( 'java', javaArgs, { cwd: DYNAMO_DB_LOCAL_DIR } );

    dynamoDBLocalProcess.stdout.on( 'data', ( data ) => {
        console.log( `DDBServer: ${ data.toString().split( '\n' ).join( '\nDDBServer: ' ) }` );
    } );

    dynamoDBLocalProcess.stderr.on( 'data', ( data ) => {
        console.error( `stderr: ${ data.toString() }` );
    } );

    dynamoDBLocalProcess.on( 'close', ( code ) => {
        if ( code !== 0 ) {
            console.error( `DynamoDB Local process exited with code ${ code }` );
        } else {
            console.log( 'DynamoDB Local stopped gracefully' );
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
                console.log( `Successfully stopped ${ port ? `process on port ${ port }` : '' } (PID: ${ pid })` );
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
            // console.error(`Error finding process on port ${port}:`, error);
            return;
        }

        const pid = stdout.trim();
        if ( ! pid ) {
            console.log( `No process found running on port ${ port }` );
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
                console.log( `Port ${ port } is active` );
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
        console.log( `Waiting for port ${ port } to become active... Attempt ${ i + 1 } of ${ retryCount }` );

        await new Promise( ( resolve ) => setTimeout( resolve, timeout ) );

        try {
            await tryConnect()
            return;
        } catch ( e ) {

        }
    }

    throw new Error( `Port ${ port } is not active after ${ retryCount } attempts` );
}

export function dDBHandleTermination( processPid: number ) {
    async function terminate() {
        console.log( "Shutting down..." );
        await dDBStop( { pid: processPid } );
    }

    process.on( "SIGINT", terminate );
    process.on( "SIGTERM", terminate );

}
