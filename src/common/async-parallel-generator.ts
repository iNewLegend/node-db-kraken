interface IManagerInterface {
    start: () => void;
    close: () => void;
}

type TConsumer<T> = AsyncGenerator<T>;

// Instead of a recursive type, use an array type with length to avoid deep circular dependency
type TTuple<T, N extends number> = number extends N ? T[] : TTupleOf<T, N>;

// Helper type that creates fixed-length tuples
type TTupleOf<
    T,
    N extends number,
    R extends unknown[] = []
> = R['length'] extends N
    ? R
    : R['length'] extends 10
      ? T[]
      : TTupleOf<T, N, [...R, T]>;

export function AsyncParallelGenerator<T, N extends number>(
    generator: () => AsyncGenerator<T>,
    numConsumers: N
): [...TTuple<TConsumer<T>, N>, IManagerInterface] {
    let started = false,
        done = false,
        consumedCount = 0,
        activeConsumers = numConsumers;

    const waitingConsumers: Set<(value: T | undefined) => void> = new Set();

    const generatorLoop = async () => {
        const gen = generator();

        // Generator loop started, active consumers `consumers`
        try {
            for await (const value of gen) {
                consumedCount = 0;
                // New value received, waiting for `consumers`.

                // Wait for all consumers to be ready
                while (waitingConsumers.size < activeConsumers && !done) {
                    await new Promise((resolve) => setTimeout(resolve, 0));
                }

                // Processing value with `${waitingConsumers.size}` consumers.
                waitingConsumers.forEach((resolver) => resolver(value));
                waitingConsumers.clear();

                // Wait for all consumers to process
                while (consumedCount < activeConsumers && !done) {
                    await new Promise((resolve) => setTimeout(resolve, 0));
                }
            }
        } finally {
            done = true;
            waitingConsumers.forEach((resolver) => resolver(undefined));
        }
    };

    function createConsumer(): AsyncGenerator<T> {
        return {
            async next(): Promise<IteratorResult<T>> {
                if (done) {
                    return { value: undefined, done: true };
                }

                const value = await new Promise<T | undefined>((resolve) => {
                    // Consumer waiting for value.
                    waitingConsumers.add(resolve);
                });

                if (value === undefined) {
                    return { value: undefined, done: true };
                }

                consumedCount++;

                // Consumer processed value. Total processed: `${consumedCount}/${activeConsumers}`.

                return { value, done: false };
            },
            async return(value?: T): Promise<IteratorResult<T>> {
                activeConsumers--;
                return { value, done: true };
            },
            async throw(error?: any): Promise<IteratorResult<T>> {
                throw error;
            },
            [Symbol.asyncIterator]() {
                return this;
            }
        };
    }

    return [
        ...Array.from({ length: numConsumers }, () => createConsumer()),
        {
            start() {
                if (!started) {
                    started = true;
                    generatorLoop();
                }
            },
            close() {
                done = true;
                waitingConsumers.clear();
            }
        }
    ] as [...TTuple<TConsumer<T>, N>, IManagerInterface];
}
