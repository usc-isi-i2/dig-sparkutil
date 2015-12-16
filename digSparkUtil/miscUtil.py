#!/usr/bin/env python

# Filename: miscUtil.py

from datetime import datetime

def seconds_since_epoch():
    return int(round((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds()))
