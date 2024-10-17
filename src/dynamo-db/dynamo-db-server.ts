import { exec, spawn } from "node:child_process";
import * as fs from "node:fs";
import * as path from "node:path";
import * as crypto from "node:crypto";
import * as util from "node:util";
import * as tar from "tar";

import net from "node:net";

import fetch from "node-fetch";

const NODE_MODULES_DIR_PATH = path.dirname( process.env[ "npm_package_json" ]! ) + "/node_modules";

const debug = util.debug( 'dynamodb-local:server' );

interface IDynamoDBLocalServerArgs {
    packageURL?: string,
    packageChecksum?: string,
    packageTmpPath?: string,
    packageExtractPath?: string,
    port?: number,
    sharedDb?: boolean,
    executeArgs?: string[]
}

const DEFAULT_LOCAL_SERVER_ARGS: Required<IDynamoDBLocalServerArgs> = {
    packageURL: "https://d1ni2b6xgvw0s0.cloudfront.net/v2.x/dynamodb_local_latest.tar.gz",
    packageChecksum: "f5296028d645bb2d3f99fede0a36945956eb7386174430e75c00e6fb1b34e78d",
    packageTmpPath: "/tmp/dynamodb_local_latest.tar.gz",
    packageExtractPath: NODE_MODULES_DIR_PATH + "/.bin/dynamodb-local",
    port: 8000,
    sharedDb: false,
    executeArgs: []
}

export class DynamoDBLocalServer {
    private args = DEFAULT_LOCAL_SERVER_ARGS

    private processPID: number | null = null;

    private isTerminateProcessing = false;

    public constructor( args: IDynamoDBLocalServerArgs = {} ) {
        Object.assign( this.args, DEFAULT_LOCAL_SERVER_ARGS, args );

        const terminate = async () => {
            if ( this.isTerminateProcessing ) {
                return;
            }
            this.isTerminateProcessing = true;
            console.info( "\nShutting down..." );
            await this.stop();
        }

        process.on( "SIGINT", terminate.bind( this ) );
        process.on( "SIGTERM", terminate.bind( this ) );
    }

    private handleEmptyDirectoryCleanup() {
        try {
            if ( fs.readdirSync( this.args.packageExtractPath ).length === 0 ) {
                // Remove the LOCAL_DIR if it's empty
                fs.rmdirSync( this.args.packageExtractPath, { recursive: true } );
            }
        } catch ( e ) {
        }
    }

    private calculateChecksum( filePath: string ): Promise<string> {
        return new Promise( ( resolve, reject ) => {
            const hash = crypto.createHash( 'sha256' );
            const stream = fs.createReadStream( filePath );

            stream.on( 'data', data => hash.update( data ) );
            stream.on( 'end', () => resolve( hash.digest( 'hex' ) ) );
            stream.on( 'error', reject );
        } );
    }

    private validChecksum( filePath: string, expectedChecksum: string = this.args.packageChecksum ) {
        return new Promise<void>( async ( resolve, reject ) => {
            try {
                const calculatedChecksum = await this.calculateChecksum( filePath );
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

    private extractPackage( filePath: string, options = { ensurePathExists: true } ) {
        if ( options.ensurePathExists && ! fs.existsSync( this.args.packageExtractPath ) ) {
            fs.mkdirSync( this.args.packageExtractPath, { recursive: true } );
        }

        return new Promise<void>( ( resolve, reject ) => {
            tar.x( {
                    file: filePath,
                    cwd: this.args.packageExtractPath
                }, ( error ) => {
                    if ( error ) {
                        reject( `Extraction error: ${ error.message }` );
                    } else {
                        debug( `Extraction completed.` );
                        resolve();
                    }
                }
            )
        } )
    }

    private async downloadWithProgress( url: string = this.args.packageURL, targetPath: string = this.args.packageExtractPath ) {
        const res = await fetch( url );
        if ( ! res.ok ) {
            throw new Error( `Failed to download DynamoDB Local: ${ res.statusText }` );
        }

        const totalBytes = parseInt( res.headers.get( 'content-length' ) || '0', 10 );
        if ( totalBytes === 0 ) {
            throw new Error( 'Unable to determine the total download size.' );
        }

        const fileStream = fs.createWriteStream( targetPath );
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
                resolve( targetPath );
            } );

            res.body.on( 'error', reject );
            fileStream.on( 'error', reject );
        } );
    }

    public async downloadInternals( retryCount = 3 ): Promise<void> {
        this.handleEmptyDirectoryCleanup();

        if ( fs.existsSync( this.args.packageTmpPath ) ) {
            const result = await this.validChecksum( this.args.packageTmpPath )
                .then( () => "re-extract" )
                .catch( () => "re-download" );

            if ( result === "re-download" ) {
                // If checksum doesn't match, delete the file and re-download
                fs.unlinkSync( this.args.packageTmpPath );

                return this.downloadInternals( retryCount );
            }

            if ( result === "re-extract" ) {
                return this.extractPackage( this.args.packageExtractPath );
            }

            throw new Error( "Something went wrong" );
        }

        let attempts = 0;
        while ( attempts < retryCount ) {
            try {
                // Ensure the output directory exists and check if DynamoDB Local is already installed
                if ( fs.existsSync( path.join( this.args.packageExtractPath ) ) ) {
                    return;
                }

                await this.downloadWithProgress();

                await this.validChecksum( this.args.packageTmpPath, this.args.packageChecksum );

                await this.extractPackage( this.args.packageTmpPath );
            } catch ( error ) {
                console.error( `Attempt ${ attempts + 1 }/${ retryCount } failed: ${ util.inspect( error, { depth: null } ) }` );
                attempts++;

                if ( attempts >= retryCount ) {
                    throw new Error( `Failed to download and install DynamoDB Local after ${ retryCount } attempts.` );
                }
            }
        }
    }

    public async start() {
        const dynamoInternalsJAR = path.join( this.args.packageExtractPath, 'DynamoDBLocal.jar' );

        if ( ! fs.existsSync( dynamoInternalsJAR ) ) {
            throw new Error( `DynamoDB Internals JAR file not found at ${ dynamoInternalsJAR }` );
        }

        const javaArgs = [
            '-Djava.library.path=./DynamoDBLocal_lib',
            '-jar',
            dynamoInternalsJAR,
            '-port',
            this.args.port.toString(),
            ... this.args.executeArgs,
        ];

        if ( this.args.sharedDb ) {
            javaArgs.push( '-sharedDb' );
        }

        debug( `Launching DynamoDB Local with arguments: ${ javaArgs.join( ' ' ) }` );

        const dynamoDBLocalProcess =
            spawn( 'java', javaArgs, { cwd: this.args.packageExtractPath } );

        dynamoDBLocalProcess.stdout.on( 'data', ( data ) => {
            debug( `DDBServer: ${ data.toString().split( '\n' ).join( '\nDDBServer: ' ) }` );
        } );

        dynamoDBLocalProcess.stderr.on( 'data', ( data ) => {
            throw new Error( `stderr: ${ data.toString() }` );
        } );

        dynamoDBLocalProcess.on( 'close', ( code ) => {
            debug( `DynamoDB Local exited with code ${ code }` );
        } );

        return dynamoDBLocalProcess;
    }

    public async stop() {
        const port = this.args.port,
            pid = this.processPID;

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

    public async waitForServerListening( retryCount = 3, timeout = 2000 ) {
        const port = this.args.port;

        async function tryConnect() {
            return await new Promise<Boolean>( ( resolve, reject ) => {
                const socket = new net.Socket();

                socket.connect( port, "localhost", () => {
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

        for ( let i = 0 ; i < ( retryCount + 1 ) ; i++ ) {
            try {
                await tryConnect()
                return;
            } catch ( e ) {
            }

            debug( `Waiting for port ${ port } to become active... Attempt ${ i + 1 } of ${ retryCount }` );

            await new Promise( ( resolve ) => setTimeout( resolve, timeout ) );

        }

        throw new Error( `Port ${ port } is not active after ${ retryCount } attempts` );
    }
}
