def to_chunks(table, chunk_size):
    for i in range(0, len(table), chunk_size):
        yield table[i : i + chunk_size]


def to_split(table, split_number):
    k, m = divmod(len(table), split_number)
    return (table[i * k + min(i, m) : (i + 1) * k + min(i + 1, m)] for i in range(split_number))


def first(iterable, condition):
    return next((x for x in iterable if condition(x)), None)
