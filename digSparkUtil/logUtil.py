#!/usr/bin/env python

### Configure python standard logging to use same file/format as log4j logger

import logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(levelname)s  %(name)s:%(lineno)d - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
