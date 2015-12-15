#!/usr/bin/env python

import collections

def as_dict(likeDict):
    if isinstance(likeDict, collections.Mapping):
        return likeDict
    else:
        try:
            return likeDict.__dict__
        except:
            raise TypeError("Can't coerce {} to act like a dict".format(likeDict))

def dict_minus(d, key):
    """Delete key from dict if exists, returning resulting dict"""
    # make shallow copy
    d = dict(d)
    try:
        del d[key]
    except:
        pass
    return d
