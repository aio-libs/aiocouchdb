# -*- coding: utf-8 -*-
#
# Copyright (C) 2015 Alexander Shorin
# All rights reserved.
#
# This software is licensed as described in the file LICENSE, which
# you should have received as part of this distribution.
#

from aiohttp.hdrs import *
from aiohttp.multidict import upstr

#: http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.5.html
ACCEPT_RANGES = upstr('ACCEPT-RANGES')

