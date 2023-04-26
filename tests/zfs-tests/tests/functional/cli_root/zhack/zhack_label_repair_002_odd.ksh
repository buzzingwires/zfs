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
# Test whether zhack repair undetach can recover
# detached drives on devices of odd size, but not
# repair corrupted checksums.
#
# Strategy:
#
# 1. Create pool on a loopback device with some test data
# 2. Detach either device from the mirror
# 3. Export the pool
# 4. Remove the non-detached device and its backing file
# 5. Verify that the remaining detached device cannot be imported
# 6. Verify that it cannot be imported after using zhack repair label
# 7. Use zhack repair undetach on device
# 8. Verify that the detached device can be imported and that data is intact

. "$STF_SUITE"/tests/functional/cli_root/zhack/library.kshlib

run_test_two "$MINVDEVSIZE_ODD"
