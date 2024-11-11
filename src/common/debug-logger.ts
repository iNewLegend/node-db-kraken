import * as Console from "node:console";
import process from 'node:process';

const entitiesPatterns = ( process.env.DEBUG_MODULES ?? '' )
    .toLowerCase()
    .split( ',' )
    .map( ( entity: string ) => entity.trim() );

export class DebugLogger extends Console.Console {
    private static instances: Record<string, DebugLogger> = {};

    public static isEnabled( entityName: string ): boolean {
        const entityPattern = entityName.toLowerCase();

        // Check for explicit disabling via `-namespace`.
        const isExplicitlyDisabled = entitiesPatterns.some(
            ( pattern: string ) =>
                pattern.startsWith( '-' ) &&
                entityPattern.startsWith( pattern.slice( 1 ) )
        );

        if ( isExplicitlyDisabled ) {
            return false;
        }

        // Check for enabling via 'namespace:*' or exact match
        return entitiesPatterns.some( ( pattern ) => {
            if ( pattern.endsWith( '*' ) ) {
                return entityName.startsWith( pattern.slice( 0, -1 ) );
            }
        } );
    }

    public static create( name: string ) {
        const [ namespaceName, entityName ] = name.split( ':' );

        if ( ! namespaceName || ! entityName ) {
            throw new Error(
                `Invalid namespace name: ${ name }, use namespace:entity`
            );
        }

        if ( ! this.isEnabled( name ) ) {
            return () => {
            };
        }

        if ( ! this.instances[ entityName ] ) {
            this.instances[ entityName ] = new this( process.stdout );
        }

        return this.instances[ entityName ].createCallback( entityName );
    }

    createCallback( entityName: string ): ( callback: () => any[] ) => void {
        return ( callback: () => any[] ) => {
            const args = callback();

            if ( 'string' === typeof args[ 0 ] ) {
                if ( args.length === 1 ) {
                    this.debug( entityName + ' -> ' + args[ 0 ] );
                    return;
                }

                this.debug( entityName + ' -> ' + args[ 0 ], {
                    args: args.slice( 1 )
                } );
                return;
            }

            this.debug( entityName, { args } );
        };
    }
}
