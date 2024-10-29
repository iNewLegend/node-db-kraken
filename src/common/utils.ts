export function isTrackingColumn( columnName: string ): boolean {
    return /_[A-Z]+_SYNCED$/.test( columnName ) || /_[A-Z]+_DELETED$/.test( columnName );
}
