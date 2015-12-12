#!/usr/bin/env python

def as_dict(likeDict):
    if isinstance(likeDict, dict):
        return likeDict
    else:
        try:
            return likeDict.__dict__
        except:
            raise TypeError("Can't coerce {} to act like a dict".format(likeDict))
