#!/usr/bin/env python3
##############################################################################
# Copyright (c) 2020, Hajime Nakagami<nakagami@gmail.com>
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#  this list of conditions and the following disclaimer in the documentation
#  and/or other materials provided with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
##############################################################################
r"""
curl -O https://raw.githubusercontent.com/FirebirdSQL/firebird/master/src/common/TimeZones.h
grep ^$'\t' TimeZones.h | sed 's/^.*"\(.*\)".*$/\1/' | ./make_tz_map.py > ../firebirdsql/tz_map.py
"""
import sys
timezone_name_by_id = {}
timezone_id_by_name = {}

timezone_id = 65535
for tz_name in sys.stdin:
    tz_name = tz_name.strip()
    timezone_name_by_id[timezone_id] = tz_name
    timezone_id_by_name[tz_name] = timezone_id
    timezone_id -= 1

print("# generated by misc/make_tz_map.py")
print("timezone_id_by_name = {")
for k, v in timezone_id_by_name.items():
    print('    "{}": {},'.format(k, v))
print("}")
print()
print("timezone_name_by_id = {")
for k, v in timezone_name_by_id.items():
    print('    {}: "{}",'.format(k, v))
print("}")
