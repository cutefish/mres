import inspect
import os
import sys

currdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
sys.path.append('%s/../util' % (currdir))
