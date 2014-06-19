#!/usr/bin/python

import sys

log = sys.stderr
output = sys.stdout

rec = sys.stdin.read()
log.write(rec)
output.write(rec)
