export function snowflakeFormatTypeWithLength( col: any ): string {
    if ( col.CHARACTER_MAXIMUM_LENGTH ) {
        return `${ col.DATA_TYPE }(${ col.CHARACTER_MAXIMUM_LENGTH })`;
    }
    if ( col.NUMERIC_PRECISION ) {
        return `${ col.DATA_TYPE }(${ col.NUMERIC_PRECISION },${ col.NUMERIC_SCALE })`;
    }
    return col.DATA_TYPE;
}
