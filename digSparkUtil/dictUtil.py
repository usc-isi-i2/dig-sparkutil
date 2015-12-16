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

def dict_minus(d, *keys):
    """Delete key(s) from dict if exists, returning resulting dict"""
    # make shallow copy
    d = dict(d)
    for key in keys:
        try:
            del d[key]
        except:
            pass
    return d

def merge_dicts(*dicts):
    """return dict created by updating each dict in sequence, last key wins"""
    result = {}
    for d in dicts:
        result.update(d)
    return result
