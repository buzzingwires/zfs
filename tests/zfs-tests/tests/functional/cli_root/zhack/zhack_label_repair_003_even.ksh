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
# detached drives with corrupted checksums on devices of even size.
#
# Strategy:
#
# 1. Create pool with a file-based mirror vdev with some test data
# 2. Detach either device from the mirror
# 3. Corrupt checksums on detached device
# 4. Export the pool
# 5. Delete the non-detached device
# 6. Verify that the remaining detached device cannot be imported
# 7. Use zhack to repair checksums and uberblocks in the pool
# 8. Verify that the detached device can be imported and that data is intact

. "$STF_SUITE"/tests/functional/cli_root/zhack/library.kshlib

run_test_three "$MINVDEVSIZE"
