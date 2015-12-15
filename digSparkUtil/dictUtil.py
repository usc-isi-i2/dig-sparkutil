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
