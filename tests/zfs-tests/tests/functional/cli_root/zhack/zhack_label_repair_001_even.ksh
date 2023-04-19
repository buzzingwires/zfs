#!/bin/ksh

#
# This file and its contents are supplied under the terms of the
# Common Development and Distribution License ("CDDL"), version 1.0.
# You may only use this file in accordance with the terms of version
# 1.0 of the CDDL.
#

#
# Description:
#
# Test whether zhack label repair can recover
# corrupted checksums on devices of even size.
#
# Strategy:
#
# 1. Create pool with a file-based vdev
# 2. Corrupt all labels checksums in the pool
# 3. Check that pool cannot be imported
# 4. Use zhack to repair labels checksums in the pool
# 5. Check that pool can be imported and that data is intact

. "$STF_SUITE"/tests/functional/cli_root/zhack/library.kshlib

run_test_one "$MINVDEVSIZE"
