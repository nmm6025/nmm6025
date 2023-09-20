# The same mapper function as in q1.py
def mapper(key, value):
    for word in value.split():
        if 'f' in word or 't' in word:
            yield (word, 1)

# The new reducer function for q2.py
def reducer(key, values):
    total = sum(values)
    if total >= 2:
        yield (key, total)

