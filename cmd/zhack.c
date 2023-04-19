/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or https://opensource.org/licenses/CDDL-1.0.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */

/*
 * Copyright (c) 2011, 2015 by Delphix. All rights reserved.
 * Copyright (c) 2013 Steven Hartland. All rights reserved.
 */

/*
 * zhack is a debugging tool that can write changes to ZFS pool using libzpool
 * for testing purposes. Altering pools with zhack is unsupported and may
 * result in corrupted pools.
 */

#include <zfs_prop.h>
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/zfs_context.h>
#include <sys/spa.h>
#include <sys/spa_impl.h>
#include <sys/dmu.h>
#include <sys/zap.h>
#include <sys/zfs_znode.h>
#include <sys/dsl_synctask.h>
#include <sys/vdev.h>
#include <sys/vdev_impl.h>
#include <sys/fs/zfs.h>
#include <sys/dmu_objset.h>
#include <sys/dsl_pool.h>
#include <sys/zio_checksum.h>
#include <sys/zio_compress.h>
#include <sys/zfeature.h>
#include <sys/dmu_tx.h>
#include <zfeature_common.h>
#include <libzutil.h>

static importargs_t g_importargs;
static char *g_pool;
static boolean_t g_readonly;

static __attribute__((noreturn)) void
usage(void)
{
	(void) fprintf(stderr,
	    "Usage: zhack [-c cachefile] [-d dir] <subcommand> <args> ...\n"
	    "where <subcommand> <args> is one of the following:\n"
	    "\n");

	(void) fprintf(stderr,
	    "    feature stat <pool>\n"
	    "        print information about enabled features\n"
	    "    feature enable [-r] [-d desc] <pool> <feature>\n"
	    "        add a new enabled feature to the pool\n"
	    "        -d <desc> sets the feature's description\n"
	    "        -r set read-only compatible flag for feature\n"
	    "    feature ref [-md] <pool> <feature>\n"
	    "        change the refcount on the given feature\n"
	    "        -d decrease instead of increase the refcount\n"
	    "        -m add the feature to the label if increasing refcount\n"
	    "\n"
	    "    <feature> : should be a feature guid\n"
	    "\n"
	    "    label repair <device>\n"
	    "        repair corrupted label checksums\n"
	    "\n"
	    "    <device> : path to vdev\n");
	exit(1);
}


static __attribute__((format(printf, 3, 4))) __attribute__((noreturn)) void
fatal(spa_t *spa, const void *tag, const char *fmt, ...)
{
	va_list ap;

	if (spa != NULL) {
		spa_close(spa, tag);
		(void) spa_export(g_pool, NULL, B_TRUE, B_FALSE);
	}

	va_start(ap, fmt);
	(void) fputs("zhack: ", stderr);
	(void) vfprintf(stderr, fmt, ap);
	va_end(ap);
	(void) fputc('\n', stderr);

	exit(1);
}

static int
space_delta_cb(dmu_object_type_t bonustype, const void *data,
    zfs_file_info_t *zoi)
{
	(void) data, (void) zoi;

	/*
	 * Is it a valid type of object to track?
	 */
	if (bonustype != DMU_OT_ZNODE && bonustype != DMU_OT_SA)
		return (ENOENT);
	(void) fprintf(stderr, "modifying object that needs user accounting");
	abort();
}

/*
 * Target is the dataset whose pool we want to open.
 */
static void
zhack_import(char *target, boolean_t readonly)
{
	nvlist_t *config;
	nvlist_t *props;
	int error;

	kernel_init(readonly ? SPA_MODE_READ :
	    (SPA_MODE_READ | SPA_MODE_WRITE));

	dmu_objset_register_type(DMU_OST_ZFS, space_delta_cb);

	g_readonly = readonly;
	g_importargs.can_be_active = readonly;
	g_pool = strdup(target);

	libpc_handle_t lpch = {
		.lpc_lib_handle = NULL,
		.lpc_ops = &libzpool_config_ops,
		.lpc_printerr = B_TRUE
	};
	error = zpool_find_config(&lpch, target, &config, &g_importargs);
	if (error)
		fatal(NULL, FTAG, "cannot import '%s'", target);

	props = NULL;
	if (readonly) {
		VERIFY(nvlist_alloc(&props, NV_UNIQUE_NAME, 0) == 0);
		VERIFY(nvlist_add_uint64(props,
		    zpool_prop_to_name(ZPOOL_PROP_READONLY), 1) == 0);
	}

	zfeature_checks_disable = B_TRUE;
	error = spa_import(target, config, props,
	    (readonly ?  ZFS_IMPORT_SKIP_MMP : ZFS_IMPORT_NORMAL));
	fnvlist_free(config);
	zfeature_checks_disable = B_FALSE;
	if (error == EEXIST)
		error = 0;

	if (error)
		fatal(NULL, FTAG, "can't import '%s': %s", target,
		    strerror(error));
}

static void
zhack_spa_open(char *target, boolean_t readonly, const void *tag, spa_t **spa)
{
	int err;

	zhack_import(target, readonly);

	zfeature_checks_disable = B_TRUE;
	err = spa_open(target, spa, tag);
	zfeature_checks_disable = B_FALSE;

	if (err != 0)
		fatal(*spa, FTAG, "cannot open '%s': %s", target,
		    strerror(err));
	if (spa_version(*spa) < SPA_VERSION_FEATURES) {
		fatal(*spa, FTAG, "'%s' has version %d, features not enabled",
		    target, (int)spa_version(*spa));
	}
}

static void
dump_obj(objset_t *os, uint64_t obj, const char *name)
{
	zap_cursor_t zc;
	zap_attribute_t za;

	(void) printf("%s_obj:\n", name);

	for (zap_cursor_init(&zc, os, obj);
	    zap_cursor_retrieve(&zc, &za) == 0;
	    zap_cursor_advance(&zc)) {
		if (za.za_integer_length == 8) {
			ASSERT(za.za_num_integers == 1);
			(void) printf("\t%s = %llu\n",
			    za.za_name, (u_longlong_t)za.za_first_integer);
		} else {
			ASSERT(za.za_integer_length == 1);
			char val[1024];
			VERIFY(zap_lookup(os, obj, za.za_name,
			    1, sizeof (val), val) == 0);
			(void) printf("\t%s = %s\n", za.za_name, val);
		}
	}
	zap_cursor_fini(&zc);
}

static void
dump_mos(spa_t *spa)
{
	nvlist_t *nv = spa->spa_label_features;
	nvpair_t *pair;

	(void) printf("label config:\n");
	for (pair = nvlist_next_nvpair(nv, NULL);
	    pair != NULL;
	    pair = nvlist_next_nvpair(nv, pair)) {
		(void) printf("\t%s\n", nvpair_name(pair));
	}
}

static void
zhack_do_feature_stat(int argc, char **argv)
{
	spa_t *spa;
	objset_t *os;
	char *target;

	argc--;
	argv++;

	if (argc < 1) {
		(void) fprintf(stderr, "error: missing pool name\n");
		usage();
	}
	target = argv[0];

	zhack_spa_open(target, B_TRUE, FTAG, &spa);
	os = spa->spa_meta_objset;

	dump_obj(os, spa->spa_feat_for_read_obj, "for_read");
	dump_obj(os, spa->spa_feat_for_write_obj, "for_write");
	dump_obj(os, spa->spa_feat_desc_obj, "descriptions");
	if (spa_feature_is_active(spa, SPA_FEATURE_ENABLED_TXG)) {
		dump_obj(os, spa->spa_feat_enabled_txg_obj, "enabled_txg");
	}
	dump_mos(spa);

	spa_close(spa, FTAG);
}

static void
zhack_feature_enable_sync(void *arg, dmu_tx_t *tx)
{
	spa_t *spa = dmu_tx_pool(tx)->dp_spa;
	zfeature_info_t *feature = arg;

	feature_enable_sync(spa, feature, tx);

	spa_history_log_internal(spa, "zhack enable feature", tx,
	    "name=%s flags=%u",
	    feature->fi_guid, feature->fi_flags);
}

static void
zhack_do_feature_enable(int argc, char **argv)
{
	int c;
	char *desc, *target;
	spa_t *spa;
	objset_t *mos;
	zfeature_info_t feature;
	const spa_feature_t nodeps[] = { SPA_FEATURE_NONE };

	/*
	 * Features are not added to the pool's label until their refcounts
	 * are incremented, so fi_mos can just be left as false for now.
	 */
	desc = NULL;
	feature.fi_uname = "zhack";
	feature.fi_flags = 0;
	feature.fi_depends = nodeps;
	feature.fi_feature = SPA_FEATURE_NONE;

	optind = 1;
	while ((c = getopt(argc, argv, "+rd:")) != -1) {
		switch (c) {
		case 'r':
			feature.fi_flags |= ZFEATURE_FLAG_READONLY_COMPAT;
			break;
		case 'd':
			if (desc != NULL)
				free(desc);
			desc = strdup(optarg);
			break;
		default:
			usage();
			break;
		}
	}

	if (desc == NULL)
		desc = strdup("zhack injected");
	feature.fi_desc = desc;

	argc -= optind;
	argv += optind;

	if (argc < 2) {
		(void) fprintf(stderr, "error: missing feature or pool name\n");
		usage();
	}
	target = argv[0];
	feature.fi_guid = argv[1];

	if (!zfeature_is_valid_guid(feature.fi_guid))
		fatal(NULL, FTAG, "invalid feature guid: %s", feature.fi_guid);

	zhack_spa_open(target, B_FALSE, FTAG, &spa);
	mos = spa->spa_meta_objset;

	if (zfeature_is_supported(feature.fi_guid))
		fatal(spa, FTAG, "'%s' is a real feature, will not enable",
		    feature.fi_guid);
	if (0 == zap_contains(mos, spa->spa_feat_desc_obj, feature.fi_guid))
		fatal(spa, FTAG, "feature already enabled: %s",
		    feature.fi_guid);

	VERIFY0(dsl_sync_task(spa_name(spa), NULL,
	    zhack_feature_enable_sync, &feature, 5, ZFS_SPACE_CHECK_NORMAL));

	spa_close(spa, FTAG);

	free(desc);
}

static void
feature_incr_sync(void *arg, dmu_tx_t *tx)
{
	spa_t *spa = dmu_tx_pool(tx)->dp_spa;
	zfeature_info_t *feature = arg;
	uint64_t refcount;

	VERIFY0(feature_get_refcount_from_disk(spa, feature, &refcount));
	feature_sync(spa, feature, refcount + 1, tx);
	spa_history_log_internal(spa, "zhack feature incr", tx,
	    "name=%s", feature->fi_guid);
}

static void
feature_decr_sync(void *arg, dmu_tx_t *tx)
{
	spa_t *spa = dmu_tx_pool(tx)->dp_spa;
	zfeature_info_t *feature = arg;
	uint64_t refcount;

	VERIFY0(feature_get_refcount_from_disk(spa, feature, &refcount));
	feature_sync(spa, feature, refcount - 1, tx);
	spa_history_log_internal(spa, "zhack feature decr", tx,
	    "name=%s", feature->fi_guid);
}

static void
zhack_do_feature_ref(int argc, char **argv)
{
	int c;
	char *target;
	boolean_t decr = B_FALSE;
	spa_t *spa;
	objset_t *mos;
	zfeature_info_t feature;
	const spa_feature_t nodeps[] = { SPA_FEATURE_NONE };

	/*
	 * fi_desc does not matter here because it was written to disk
	 * when the feature was enabled, but we need to properly set the
	 * feature for read or write based on the information we read off
	 * disk later.
	 */
	feature.fi_uname = "zhack";
	feature.fi_flags = 0;
	feature.fi_desc = NULL;
	feature.fi_depends = nodeps;
	feature.fi_feature = SPA_FEATURE_NONE;

	optind = 1;
	while ((c = getopt(argc, argv, "+md")) != -1) {
		switch (c) {
		case 'm':
			feature.fi_flags |= ZFEATURE_FLAG_MOS;
			break;
		case 'd':
			decr = B_TRUE;
			break;
		default:
			usage();
			break;
		}
	}
	argc -= optind;
	argv += optind;

	if (argc < 2) {
		(void) fprintf(stderr, "error: missing feature or pool name\n");
		usage();
	}
	target = argv[0];
	feature.fi_guid = argv[1];

	if (!zfeature_is_valid_guid(feature.fi_guid))
		fatal(NULL, FTAG, "invalid feature guid: %s", feature.fi_guid);

	zhack_spa_open(target, B_FALSE, FTAG, &spa);
	mos = spa->spa_meta_objset;

	if (zfeature_is_supported(feature.fi_guid)) {
		fatal(spa, FTAG,
		    "'%s' is a real feature, will not change refcount",
		    feature.fi_guid);
	}

	if (0 == zap_contains(mos, spa->spa_feat_for_read_obj,
	    feature.fi_guid)) {
		feature.fi_flags &= ~ZFEATURE_FLAG_READONLY_COMPAT;
	} else if (0 == zap_contains(mos, spa->spa_feat_for_write_obj,
	    feature.fi_guid)) {
		feature.fi_flags |= ZFEATURE_FLAG_READONLY_COMPAT;
	} else {
		fatal(spa, FTAG, "feature is not enabled: %s", feature.fi_guid);
	}

	if (decr) {
		uint64_t count;
		if (feature_get_refcount_from_disk(spa, &feature,
		    &count) == 0 && count == 0) {
			fatal(spa, FTAG, "feature refcount already 0: %s",
			    feature.fi_guid);
		}
	}

	VERIFY0(dsl_sync_task(spa_name(spa), NULL,
	    decr ? feature_decr_sync : feature_incr_sync, &feature,
	    5, ZFS_SPACE_CHECK_NORMAL));

	spa_close(spa, FTAG);
}

static int
zhack_do_feature(int argc, char **argv)
{
	char *subcommand;

	argc--;
	argv++;
	if (argc == 0) {
		(void) fprintf(stderr,
		    "error: no feature operation specified\n");
		usage();
	}

	subcommand = argv[0];
	if (strcmp(subcommand, "stat") == 0) {
		zhack_do_feature_stat(argc, argv);
	} else if (strcmp(subcommand, "enable") == 0) {
		zhack_do_feature_enable(argc, argv);
	} else if (strcmp(subcommand, "ref") == 0) {
		zhack_do_feature_ref(argc, argv);
	} else {
		(void) fprintf(stderr, "error: unknown subcommand: %s\n",
		    subcommand);
		usage();
	}

	return (0);
}

static boolean_t
zhack_label_write(int l,
    int fd,
    int byteswap,
    void *data,
    zio_eck_t *eck,
    uint64_t offset,
    uint64_t abdsize)
{
	zio_cksum_t verifier;
	zio_cksum_t actual_cksum;
	zio_cksum_t expected_cksum;
	zio_checksum_info_t *ci;
	abd_t *abd;
	ssize_t err;

	ZIO_SET_CHECKSUM(&verifier, offset, 0, 0, 0);

	if (byteswap) {
		byteswap_uint64_array(&verifier, sizeof (zio_cksum_t));
		eck->zec_magic = BSWAP_64(eck->zec_magic);
	}

	expected_cksum = eck->zec_cksum;
	eck->zec_cksum = verifier;

	ci = &zio_checksum_table[ZIO_CHECKSUM_LABEL];
	abd = abd_get_from_buf(data, abdsize);
	ci->ci_func[byteswap](abd, abdsize, NULL, &actual_cksum);
	abd_free(abd);

	if (byteswap)
		byteswap_uint64_array(&expected_cksum, sizeof (zio_cksum_t));

	if (ZIO_CHECKSUM_EQUAL(actual_cksum, expected_cksum))
		return (B_FALSE);

	eck->zec_cksum = actual_cksum;

	err = pwrite64(fd, data, abdsize, offset);
	if (err == -1) {
		(void) fprintf(stderr, "error: cannot write label %d: %s\n",
		    l, strerror(errno));
		return (B_FALSE);
	} else if (err != abdsize) {
		(void) fprintf(stderr, "error: bad write size label %d\n", l);
		return (B_FALSE);
	} else {
		(void) fprintf(stderr,
		    "label %d: wrote %" PRIu64 " bytes at offset %" PRIu64 "\n",
		    l, abdsize, offset);
	}

	return (B_TRUE);
}

#define	ASHIFT_UBERBLOCK_SHIFT(ashift)	\
	MIN(MAX(ashift, UBERBLOCK_SHIFT), \
	MAX_UBERBLOCK_SHIFT)
#define	ASHIFT_UBERBLOCK_SIZE(ashift) \
	(1ULL << ASHIFT_UBERBLOCK_SHIFT(ashift))

#define	LABEL_SIZE 262144

static void
zhack_repair_one_label_cksum(const int fd,
    vdev_label_t * const vl,
    const uint64_t label_offset,
    const int l,
    uint32_t * const labels_repaired)
{
	const char *cfg_keys[] = { ZPOOL_CONFIG_VERSION,
	    ZPOOL_CONFIG_POOL_STATE, ZPOOL_CONFIG_GUID };
	uberblock_t *ub;
	uint64_t ashift;
	int byteswap;
	zio_eck_t *ub_eck;
	zio_eck_t *vdev_eck;
	nvlist_t *cfg;
	nvlist_t *vdev_tree_cfg;
	uint64_t txg;
	char *buf;
	size_t buflen;
	void *ub_data;
	void *vdev_data;
	ssize_t err;

	ub = (uberblock_t *)vl->vl_uberblock;

	err = pread64(fd, vl, sizeof (vdev_label_t), label_offset);
	if (err == -1) {
		(void) fprintf(stderr,
		    "error: cannot read label %d: %s\n",
		    l, strerror(errno));
		return;
	} else if (err != sizeof (vdev_label_t)) {
		(void) fprintf(stderr,
		    "error: bad label %d read size \n", l);
		return;
	}

	err = nvlist_unpack(vl->vl_vdev_phys.vp_nvlist,
	    VDEV_PHYS_SIZE - sizeof (zio_eck_t), &cfg, 0);
	if (err) {
		(void) fprintf(stderr,
		    "error: cannot unpack nvlist label %d\n", l);
		return;
	}

	if (ub->ub_txg != 0) {
		(void) fprintf(stderr,
		    "error: label %d: UB TXG of 0 expected, but got %"
		    PRIu64 "\n",
		    l, ub->ub_txg);
		return;
	}

	for (int i = 0; i < ARRAY_SIZE(cfg_keys); i++) {
		uint64_t val;
		err = nvlist_lookup_uint64(cfg, cfg_keys[i], &val);
		if (err) {
			(void) fprintf(stderr,
			    "error: label %d, %d: "
			    "cannot find nvlist key %s\n",
			    l, i, cfg_keys[i]);
			return;
		}
	}

	err = nvlist_lookup_nvlist(cfg,
	    ZPOOL_CONFIG_VDEV_TREE, &vdev_tree_cfg);
	if (err) {
		(void) fprintf(stderr,
		    "error: label %d: cannot find nvlist key %s\n",
		    l, ZPOOL_CONFIG_VDEV_TREE);
		return;
	}

	err = nvlist_lookup_uint64(vdev_tree_cfg,
	    ZPOOL_CONFIG_ASHIFT, &ashift);
	if (err) {
		(void) fprintf(stderr,
		    "error: label %d: cannot find nvlist key %s\n",
		    l, ZPOOL_CONFIG_ASHIFT);
		return;
	}

	if (ashift == 0) {
		(void) fprintf(stderr,
		    "error: label %d: nvlist key %s is zero\n",
		    l, ZPOOL_CONFIG_ASHIFT);
		return;
	}

	if (ub->ub_rootbp.blk_birth != 0) {
		txg = ub->ub_rootbp.blk_birth;
		ub->ub_txg = txg;

		if (nvlist_remove_all(cfg, ZPOOL_CONFIG_CREATE_TXG) != 0) {
			(void) fprintf(stderr,
			    "error: label %d: "
			    "Failed to remove pool creation TXG\n",
			    l);
			return;
		}

		if (nvlist_remove_all(cfg, ZPOOL_CONFIG_POOL_TXG) != 0) {
			(void) fprintf(stderr,
			    "error: label %d: Failed to remove pool TXG\n",
			    l);
			return;
		}

		if (nvlist_add_uint64(cfg, ZPOOL_CONFIG_POOL_TXG, txg) != 0) {
			(void) fprintf(stderr,
			    "error: label %d: "
			    "Failed to add pool TXG of %" PRIu64 "\n",
			    l, txg);
			return;
		}
	}

	buf = vl->vl_vdev_phys.vp_nvlist;
	buflen = VDEV_PHYS_SIZE - sizeof (zio_eck_t);

	if (nvlist_pack(cfg, &buf, &buflen, NV_ENCODE_XDR, 0) != 0) {
		(void) fprintf(stderr,
		    "error: label %d: Failed to pack nvlist\n",
		    l);
		return;
	}

	ub_data = (char *)vl + offsetof(vdev_label_t, vl_uberblock);
	ub_eck =
	    (zio_eck_t *)
	    ((char *)(ub_data) + (ASHIFT_UBERBLOCK_SIZE(ashift))) - 1;

	if (ub_eck->zec_magic != 0) {
		(void) fprintf(stderr,
		    "error: label %d: "
		    "Expected Uberblock checksum magic number to "
		    "be 0, but got %" PRIu64 "\n",
		    l, ub_eck->zec_magic);
		return;
	}

	ub_eck->zec_magic = ZEC_MAGIC;

	vdev_data = (char *)vl + offsetof(vdev_label_t, vl_vdev_phys);
	vdev_eck =
	    (zio_eck_t *)((char *)(vdev_data) + VDEV_PHYS_SIZE) - 1;

	if (vdev_eck->zec_magic == 0) {
		(void) fprintf(stderr, "error: label %d: "
		    "Expected "
		    "the nvlist checksum magic number to not be zero\n",
		    l);
		return;
	}

	byteswap =
	    (vdev_eck->zec_magic == BSWAP_64((uint64_t)ZEC_MAGIC));

	(void) fprintf(stderr, "Label %d: "
	    "byteswap returned %d for the uberblock magic of %"
	    PRIu64 " and the swapped default of %" PRIu64 "\n",
	    l, byteswap, ub->ub_magic,
	    BSWAP_64((uint64_t)UBERBLOCK_MAGIC));

	if (zhack_label_write(l, fd, byteswap,
	    ub_data, ub_eck,
	    label_offset + offsetof(vdev_label_t, vl_uberblock),
	    ASHIFT_UBERBLOCK_SIZE(ashift)))
			labels_repaired[l] |= (1 << 0);

	if (zhack_label_write(l, fd, byteswap,
	    vdev_data, vdev_eck,
	    label_offset + offsetof(vdev_label_t, vl_vdev_phys),
	    VDEV_PHYS_SIZE))
			labels_repaired[l] |= (1 << 1);

	fsync(fd);
}

static int
zhack_repair_label_cksum(int argc, char **argv)
{
	uint32_t labels_repaired[VDEV_LABELS] = {0};
	vdev_label_t labels[VDEV_LABELS] = {{{0}}};
	struct stat64 st;
	int fd;
	off_t filesize;
	uint32_t repaired = 0;

	abd_init();

	argc -= 1;
	argv += 1;

	if (argc < 1) {
		(void) fprintf(stderr, "error: missing device\n");
		usage();
	}

	if ((fd = open(argv[0], O_RDWR)) == -1)
		fatal(NULL, FTAG, "cannot open '%s': %s", argv[0],
		    strerror(errno));

	if (fstat64_blk(fd, &st) != 0)
		fatal(NULL, FTAG, "cannot stat '%s': %s", argv[0],
		    strerror(errno));

	filesize = st.st_size;
	(void) fprintf(stderr, "Calculated filesize to be %jd\n",
	    (intmax_t)filesize);

	if (filesize % LABEL_SIZE != 0) {
		filesize = (filesize / LABEL_SIZE) * LABEL_SIZE;
		(void) fprintf(stderr,
		    "Filesize is not divisible by %jd, recalculated to %jd\n",
		    (intmax_t)LABEL_SIZE, (intmax_t)filesize);
	}

	for (int l = 0; l < VDEV_LABELS; l++) {
		zhack_repair_one_label_cksum(fd,
		    &labels[l],
		    vdev_label_offset(filesize, l, 0),
		    l,
		    labels_repaired);
	}

	close(fd);

	abd_fini();

#define	LABEL_STATUS(s)	(((labels_repaired[l] & (1 << (s))) != 0) ?\
	    "repaired" : "skipped")
	for (int l = 0; l < VDEV_LABELS; l++) {
		printf("label %d: ", l);
		(void) printf("uberblock: %s ", LABEL_STATUS(0));
		(void) printf("checksum: %s\n", LABEL_STATUS(1));
		repaired |= labels_repaired[l];
	}
#undef LABEL_STATUS

	if (repaired > 0)
		return (0);

	return (1);
}

static int
zhack_do_label(int argc, char **argv)
{
	char *subcommand;
	int err;

	argc--;
	argv++;
	if (argc == 0) {
		(void) fprintf(stderr,
		    "error: no label operation specified\n");
		usage();
	}

	subcommand = argv[0];
	if (strcmp(subcommand, "repair") == 0) {
		err = zhack_repair_label_cksum(argc, argv);
	} else {
		(void) fprintf(stderr, "error: unknown subcommand: %s\n",
		    subcommand);
		usage();
	}

	return (err);
}

#define	MAX_NUM_PATHS 1024

int
main(int argc, char **argv)
{
	char *path[MAX_NUM_PATHS];
	const char *subcommand;
	int rv = 0;
	int c;

	g_importargs.path = path;

	dprintf_setup(&argc, argv);
	zfs_prop_init();

	while ((c = getopt(argc, argv, "+c:d:")) != -1) {
		switch (c) {
		case 'c':
			g_importargs.cachefile = optarg;
			break;
		case 'd':
			assert(g_importargs.paths < MAX_NUM_PATHS);
			g_importargs.path[g_importargs.paths++] = optarg;
			break;
		default:
			usage();
			break;
		}
	}

	argc -= optind;
	argv += optind;
	optind = 1;

	if (argc == 0) {
		(void) fprintf(stderr, "error: no command specified\n");
		usage();
	}

	subcommand = argv[0];

	if (strcmp(subcommand, "feature") == 0) {
		rv = zhack_do_feature(argc, argv);
	} else if (strcmp(subcommand, "label") == 0) {
		return (zhack_do_label(argc, argv));
	} else {
		(void) fprintf(stderr, "error: unknown subcommand: %s\n",
		    subcommand);
		usage();
	}

	if (!g_readonly && spa_export(g_pool, NULL, B_TRUE, B_FALSE) != 0) {
		fatal(NULL, FTAG, "pool export failed; "
		    "changes may not be committed to disk\n");
	}

	kernel_fini();

	return (rv);
}
