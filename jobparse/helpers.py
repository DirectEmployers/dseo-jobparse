from itertools import islice

def take(n, seq):
    "Return first n items of the seq as a list"
    return list(islice(seq, n))

def drop(n, seq):
    "Return seq with the first n items removed."
    return list(islice(seq, n, None, 1))

def split_by(n, seq):
    return [take(n, seq), drop(n, seq)]

def slices(seq, start=0, end=None, step=2):
    """
    Takes a sequence 'seq' and yields a 2-tuple that expresses a
    'step'-sized. Ex:

    >> x = slices(range(20), step=4)
    >> x.next()
    (0, 4)
    >> x.next()
    (4, 8)
    >> x.next()
    (8, 12)

    etc. Put another way:
    
    >> x = slices(range(20), step=4)
    >> [i for i in x]
    [(0, 4), (4, 8), (8, 12), (12, 16), (16, 20), (20, 21)]

    Note the calculation of the 'rng' variable. If 'len(seq)' is a
    multiple of 'step', we add an additional item onto the end so we make
    sure to get each item. (This is why in the example above the last
    tuple is '(20, 21)'.)
    
    """
    fun = split_by
    ls = len(seq)
    if ls < step and seq:
        yield (0, ls)
    else:
        if ls % step:
            rng = xrange(ls/step+1)
        else:
            rng = xrange(ls/step)
            
        for i in rng:
            segment = fun(step, seq[start:])
            start += step
            yield segment[0][0], segment[0][-1]
