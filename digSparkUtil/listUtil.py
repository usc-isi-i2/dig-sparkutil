#!/usr/bin/env python

def as_list(x):
    if isinstance(x, list):
        return x
    else:
        return [x]


# http://stackoverflow.com/a/2158532/2077242

import collections

def flatten(l):
    for el in l:
        if isinstance(el, collections.Iterable) and not isinstance(el, basestring):
            for sub in flatten(el):
                yield sub
        else:
            yield el

from itertools import izip_longest

def iter_chunks(iterable, n, fillvalue=None):
    args = [iter(iterable)] * n
    return izip_longest(*args, fillvalue=fillvalue)

