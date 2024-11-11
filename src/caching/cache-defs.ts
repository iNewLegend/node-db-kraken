export interface DCacheStrategy<TKey, TValue> {
    has( key: TKey ): Promise<boolean>;

    get( key: TKey ): Promise<TValue>;

    set( key: TKey, value: TValue ): Promise<void>;

    clear(): Promise<void>;
}
