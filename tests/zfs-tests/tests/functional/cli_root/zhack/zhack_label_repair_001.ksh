#!/bin/ksh
# SPDX-License-Identifier: CDDL-1.0

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
# corrupted checksums on devices of varied size,
# which have not been cleanly detached or exported.
#
# Strategy:
#
# 1. Create pool on a loopback device with some test data
# 2. Create a copy of the backing file while the pool is still imported
# 3. Export the pool
# 4. Replace the backing file with its non-cleanly exported copy
# 5. Corrupt all label checksums in the pool
# 6. Check that pool cannot be imported
# 7. Verify that it cannot be imported after using zhack label repair -u
#    to ensure that the -u option will quit on corrupted checksums
# 8. Use zhack label repair -c on device
# 9. Check that pool can be imported and that data is intact

. "$STF_SUITE"/tests/functional/cli_root/zhack/library.kshlib

run_test_one "$(get_devsize)"
