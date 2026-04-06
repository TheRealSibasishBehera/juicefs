#!/bin/bash -e
# fork.sh — E2E expectations for juicefs fork
#
# Each test encodes one expectation from the design doc.
# These are written BEFORE implementation — they will fail until
# `juicefs fork` is implemented.
#
# Run all:        bash fork.sh
# Run one:        bash fork.sh test_fork_basic
# Skip a test:    rename test_* to skip_test_*

source .github/scripts/common/common.sh

[[ -z "$META" ]] && META=sqlite3
source .github/scripts/start_meta_engine.sh
start_meta_engine $META
META_URL=$(get_meta_url $META)

# Derive a second meta URL for the forked volume.
# For sqlite3: use a sibling db file.
# For redis:   use the next db index.
get_fork_meta_url() {
    case "$META" in
        sqlite3)  echo "sqlite3:///tmp/jfs-fork-test-fork.db" ;;
        redis*)   echo "redis://127.0.0.1:6379/2" ;;
        *)        echo "sqlite3:///tmp/jfs-fork-test-fork.db" ;;
    esac
}

FORK_META_URL=$(get_fork_meta_url)

# Mount points
MNT_ORIG=/jfs-orig
MNT_FORK=/jfs-fork

# =============================================================================
# Helpers
# =============================================================================

# Cross-platform md5.
# Uses md5sum (Linux) or md5 -q (macOS).
md5_of() {
    case "$PLATFORM" in
        mac)   md5 -q "$1" ;;
        *)     md5sum "$1" | awk '{print $1}' ;;
    esac
}

# Cross-platform file size in bytes.
size_of() {
    case "$PLATFORM" in
        mac)   stat -f%z "$1" ;;
        *)     stat -c%s "$1" ;;
    esac
}

# Flush sqlite db files used as fork meta DBs.
# flush_meta.py handles redis/mysql/etc; for sqlite we just delete the file.
flush_fork_sqlite_dbs() {
    rm -f /tmp/jfs-fork-test-fork.db \
          /tmp/jfs-fork-test-fork-b.db \
          /tmp/jfs-fork-test-fork2.db \
          /tmp/jfs-fork-test-list-b.db \
          2>/dev/null || true
}

setup_two_mounts() {
    umount_jfs $MNT_ORIG "$META_URL"      2>/dev/null || true
    umount_jfs $MNT_FORK "$FORK_META_URL" 2>/dev/null || true
    python3 .github/scripts/flush_meta.py "$META_URL"
    python3 .github/scripts/flush_meta.py "$FORK_META_URL"
    flush_fork_sqlite_dbs
    rm -rf /var/jfs/myjfs      /var/jfsCache/myjfs      2>/dev/null || true
    rm -rf /var/jfs/myjfs-fork /var/jfsCache/myjfs-fork 2>/dev/null || true
    # macOS uses home-dir local storage
    rm -rf ~/.juicefs/local/myjfs      ~/.juicefs/local/myjfs-fork      2>/dev/null || true
    rm -rf ~/.juicefs/cache/myjfs      ~/.juicefs/cache/myjfs-fork      2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT_FORK
}

teardown_two_mounts() {
    umount_jfs $MNT_ORIG "$META_URL"      2>/dev/null || true
    umount_jfs $MNT_FORK "$FORK_META_URL" 2>/dev/null || true
}

assert_eq() {
    local label=$1 got=$2 want=$3
    if [[ "$got" != "$want" ]]; then
        echo "<FATAL> $label: expected '$want', got '$got'"
        exit 1
    fi
}

assert_ne() {
    local label=$1 got=$2 unwanted=$3
    if [[ "$got" == "$unwanted" ]]; then
        echo "<FATAL> $label: expected value to differ from '$unwanted'"
        exit 1
    fi
}

assert_file_exists() {
    [[ -e "$1" ]] || { echo "<FATAL> expected file to exist: $1"; exit 1; }
}

assert_file_missing() {
    [[ ! -e "$1" ]] || { echo "<FATAL> expected file to be absent: $1"; exit 1; }
}

# =============================================================================
# test_fork_basic
#
# Expectation: after fork, both volumes mount and the fork contains the same
# files that existed in the original at fork time.
# =============================================================================
test_fork_basic() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    mkdir -p $MNT_ORIG/data
    echo "hello from original" > $MNT_ORIG/data/file1.txt
    echo "shared baseline"     > $MNT_ORIG/data/baseline.txt
    dd if=/dev/urandom of=$MNT_ORIG/data/binary.bin bs=1M count=4 2>/dev/null
    ORIG_HASH=$(md5_of $MNT_ORIG/data/binary.bin)
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    assert_file_exists $MNT_FORK/data/file1.txt
    assert_file_exists $MNT_FORK/data/baseline.txt
    assert_file_exists $MNT_FORK/data/binary.bin

    assert_eq "binary hash matches at fork" \
        "$(md5_of $MNT_FORK/data/binary.bin)" "$ORIG_HASH"
    assert_eq "text content matches at fork" \
        "$(cat $MNT_FORK/data/file1.txt)" "hello from original"

    teardown_two_mounts
}

# =============================================================================
# test_fork_post_write_isolation
#
# Expectation: writes made to the original AFTER fork are not visible on the
# fork, and vice versa.
# =============================================================================
test_fork_post_write_isolation() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    echo "pre-fork" > $MNT_ORIG/pre.txt
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    echo "post-fork-orig" > $MNT_ORIG/orig-only.txt
    echo "post-fork-fork" > $MNT_FORK/fork-only.txt
    sync

    assert_file_missing $MNT_FORK/orig-only.txt
    assert_file_missing $MNT_ORIG/fork-only.txt

    teardown_two_mounts
}

# =============================================================================
# test_fork_delete_from_original_invisible_to_fork
#
# Expectation: deleting a pre-fork file from the original makes it gone from
# the original immediately, but the fork still sees it with correct content.
# =============================================================================
test_fork_delete_from_original_invisible_to_fork() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    echo "will be deleted from original" > $MNT_ORIG/deleteme.txt
    dd if=/dev/urandom of=$MNT_ORIG/deleteme.bin bs=1M count=2 2>/dev/null
    BIN_HASH=$(md5_of $MNT_ORIG/deleteme.bin)
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    rm -f $MNT_ORIG/deleteme.txt $MNT_ORIG/deleteme.bin
    sync

    # Original: files are gone immediately
    assert_file_missing $MNT_ORIG/deleteme.txt
    assert_file_missing $MNT_ORIG/deleteme.bin

    # Fork: files still exist with correct content
    assert_file_exists $MNT_FORK/deleteme.txt
    assert_file_exists $MNT_FORK/deleteme.bin
    assert_eq "fork text still readable after original delete" \
        "$(cat $MNT_FORK/deleteme.txt)" "will be deleted from original"
    assert_eq "fork binary hash unchanged after original delete" \
        "$(md5_of $MNT_FORK/deleteme.bin)" "$BIN_HASH"

    teardown_two_mounts
}

# =============================================================================
# test_fork_delete_from_fork_invisible_to_original
#
# Expectation: the reverse — deleting from fork does not affect original.
# =============================================================================
test_fork_delete_from_fork_invisible_to_original() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    echo "will be deleted from fork" > $MNT_ORIG/deleteme-fork.txt
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    rm -f $MNT_FORK/deleteme-fork.txt
    sync

    assert_file_missing $MNT_FORK/deleteme-fork.txt
    assert_file_exists  $MNT_ORIG/deleteme-fork.txt
    assert_eq "original text intact after fork delete" \
        "$(cat $MNT_ORIG/deleteme-fork.txt)" "will be deleted from fork"

    teardown_two_mounts
}

# =============================================================================
# test_fork_overwrite_isolation
#
# Expectation: overwriting a pre-fork file on one volume does not change the
# content seen on the other volume.
# =============================================================================
test_fork_overwrite_isolation() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    echo "version-1" > $MNT_ORIG/shared.txt
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    echo "version-2-orig" > $MNT_ORIG/shared.txt
    sync

    assert_eq "original sees new content" \
        "$(cat $MNT_ORIG/shared.txt)" "version-2-orig"
    assert_eq "fork still sees pre-fork content" \
        "$(cat $MNT_FORK/shared.txt)" "version-1"

    echo "version-2-fork" > $MNT_FORK/shared.txt
    sync

    assert_eq "original unaffected by fork overwrite" \
        "$(cat $MNT_ORIG/shared.txt)" "version-2-orig"
    assert_eq "fork has its own overwrite" \
        "$(cat $MNT_FORK/shared.txt)" "version-2-fork"

    teardown_two_mounts
}

# =============================================================================
# test_fork_same_path_different_content
#
# Expectation: writing known distinct data to the same path on both volumes
# after fork produces exactly what was written — no cross-contamination.
# Uses deterministic content (not /dev/urandom) so we can verify exact bytes.
# =============================================================================
test_fork_same_path_different_content() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Write known, distinct patterns to the same path on each volume
    printf 'AAAAAAAAA' > $MNT_ORIG/diverge.txt
    printf 'BBBBBBBBB' > $MNT_FORK/diverge.txt
    # Write larger binary with distinct fill bytes
    dd if=/dev/zero bs=1M count=4 2>/dev/null | tr '\0' '\101' > $MNT_ORIG/diverge.bin
    dd if=/dev/zero bs=1M count=4 2>/dev/null | tr '\0' '\102' > $MNT_FORK/diverge.bin
    sync

    assert_eq "original text has its own content" \
        "$(cat $MNT_ORIG/diverge.txt)" "AAAAAAAAA"
    assert_eq "fork text has its own content" \
        "$(cat $MNT_FORK/diverge.txt)" "BBBBBBBBB"

    # Verify binary files read back exactly what was written
    ORIG_FIRST_BYTE=$(xxd -p -l 1 $MNT_ORIG/diverge.bin)
    FORK_FIRST_BYTE=$(xxd -p -l 1 $MNT_FORK/diverge.bin)
    assert_eq "original binary has correct fill byte" "$ORIG_FIRST_BYTE" "41"
    assert_eq "fork binary has correct fill byte"     "$FORK_FIRST_BYTE" "42"

    teardown_two_mounts
}

# =============================================================================
# test_fork_truncate_isolation
#
# Expectation: truncating a pre-fork file on one volume does not change the
# size or content of that file on the other volume.
# =============================================================================
test_fork_truncate_isolation() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/truncate-me.bin bs=1M count=8 2>/dev/null
    FULL_HASH=$(md5_of $MNT_ORIG/truncate-me.bin)
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Truncate on original to 1MB
    truncate -s 1M $MNT_ORIG/truncate-me.bin
    sync

    ORIG_SIZE=$(size_of $MNT_ORIG/truncate-me.bin)
    FORK_SIZE=$(size_of $MNT_FORK/truncate-me.bin)
    assert_eq "original truncated to 1MB"      "$ORIG_SIZE" "1048576"
    assert_eq "fork size unchanged after truncate" "$FORK_SIZE" "8388608"
    assert_eq "fork content hash unchanged after truncate" \
        "$(md5_of $MNT_FORK/truncate-me.bin)" "$FULL_HASH"

    teardown_two_mounts
}

# =============================================================================
# test_fork_symlink_isolation
#
# Expectation: symlinks created on one volume post-fork do not appear on the
# other. Pre-fork symlinks are visible on both volumes and resolve correctly.
# =============================================================================
test_fork_symlink_isolation() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    echo "target-content" > $MNT_ORIG/target.txt
    ln -s target.txt $MNT_ORIG/pre-fork-link
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Pre-fork symlink visible and resolves correctly on both volumes
    assert_file_exists $MNT_ORIG/pre-fork-link
    assert_file_exists $MNT_FORK/pre-fork-link
    assert_eq "pre-fork symlink resolves on original" \
        "$(cat $MNT_ORIG/pre-fork-link)" "target-content"
    assert_eq "pre-fork symlink resolves on fork" \
        "$(cat $MNT_FORK/pre-fork-link)" "target-content"

    # Post-fork symlinks are isolated
    ln -s target.txt $MNT_ORIG/orig-link
    ln -s target.txt $MNT_FORK/fork-link
    sync

    assert_file_exists  $MNT_ORIG/orig-link
    assert_file_missing $MNT_FORK/orig-link
    assert_file_exists  $MNT_FORK/fork-link
    assert_file_missing $MNT_ORIG/fork-link

    # Delete pre-fork symlink from original — fork unaffected
    rm -f $MNT_ORIG/pre-fork-link
    sync
    assert_file_missing $MNT_ORIG/pre-fork-link
    assert_file_exists  $MNT_FORK/pre-fork-link
    assert_eq "fork symlink still resolves after original deletes it" \
        "$(cat $MNT_FORK/pre-fork-link)" "target-content"

    teardown_two_mounts
}

# =============================================================================
# test_fork_hardlink_isolation
#
# Expectation: a pre-fork hardlink deleted on one volume (one name removed)
# does not affect the other volume's view of either name. The surviving name
# still reads back the same content.
# =============================================================================
test_fork_hardlink_isolation() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    echo "hardlink-content" > $MNT_ORIG/hardlink-a.txt
    ln $MNT_ORIG/hardlink-a.txt $MNT_ORIG/hardlink-b.txt
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Both names visible on both volumes
    assert_file_exists $MNT_ORIG/hardlink-a.txt
    assert_file_exists $MNT_ORIG/hardlink-b.txt
    assert_file_exists $MNT_FORK/hardlink-a.txt
    assert_file_exists $MNT_FORK/hardlink-b.txt

    # Remove one name from original
    rm -f $MNT_ORIG/hardlink-a.txt
    sync

    # Original: a gone, b still has content
    assert_file_missing $MNT_ORIG/hardlink-a.txt
    assert_eq "original hardlink-b still readable" \
        "$(cat $MNT_ORIG/hardlink-b.txt)" "hardlink-content"

    # Fork: both names intact
    assert_file_exists $MNT_FORK/hardlink-a.txt
    assert_file_exists $MNT_FORK/hardlink-b.txt
    assert_eq "fork hardlink-a still readable" \
        "$(cat $MNT_FORK/hardlink-a.txt)" "hardlink-content"
    assert_eq "fork hardlink-b still readable" \
        "$(cat $MNT_FORK/hardlink-b.txt)" "hardlink-content"

    teardown_two_mounts
}

# =============================================================================
# test_fork_xattr_isolation
#
# Expectation: setting an xattr on one volume post-fork does not affect the
# xattr value seen on the other volume for the same file.
# =============================================================================
test_fork_xattr_isolation() {
    # xattr requires setfattr/getfattr (attr package on Linux, xattr on mac)
    if ! command -v setfattr &>/dev/null && ! command -v xattr &>/dev/null; then
        echo "skipping test_fork_xattr_isolation: no xattr tooling available"
        return
    fi

    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report --enable-xattr
    sleep 1

    echo "xattr-test" > $MNT_ORIG/xattr-file.txt
    # Set a pre-fork xattr
    case "$PLATFORM" in
        mac)  xattr -w user.prefork "prefork-value" $MNT_ORIG/xattr-file.txt ;;
        *)    setfattr -n user.prefork -v "prefork-value" $MNT_ORIG/xattr-file.txt ;;
    esac
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report --enable-xattr
    sleep 1

    # Pre-fork xattr visible on both
    case "$PLATFORM" in
        mac)
            ORIG_PRE=$(xattr -p user.prefork $MNT_ORIG/xattr-file.txt)
            FORK_PRE=$(xattr -p user.prefork $MNT_FORK/xattr-file.txt)
            ;;
        *)
            ORIG_PRE=$(getfattr -n user.prefork --only-values $MNT_ORIG/xattr-file.txt)
            FORK_PRE=$(getfattr -n user.prefork --only-values $MNT_FORK/xattr-file.txt)
            ;;
    esac
    assert_eq "pre-fork xattr on original" "$ORIG_PRE" "prefork-value"
    assert_eq "pre-fork xattr on fork"     "$FORK_PRE" "prefork-value"

    # Set different post-fork xattrs on each volume
    case "$PLATFORM" in
        mac)
            xattr -w user.postfork "orig-postfork" $MNT_ORIG/xattr-file.txt
            xattr -w user.postfork "fork-postfork" $MNT_FORK/xattr-file.txt
            ;;
        *)
            setfattr -n user.postfork -v "orig-postfork" $MNT_ORIG/xattr-file.txt
            setfattr -n user.postfork -v "fork-postfork" $MNT_FORK/xattr-file.txt
            ;;
    esac
    sync

    case "$PLATFORM" in
        mac)
            ORIG_POST=$(xattr -p user.postfork $MNT_ORIG/xattr-file.txt)
            FORK_POST=$(xattr -p user.postfork $MNT_FORK/xattr-file.txt)
            ;;
        *)
            ORIG_POST=$(getfattr -n user.postfork --only-values $MNT_ORIG/xattr-file.txt)
            FORK_POST=$(getfattr -n user.postfork --only-values $MNT_FORK/xattr-file.txt)
            ;;
    esac
    assert_eq "original post-fork xattr isolated" "$ORIG_POST" "orig-postfork"
    assert_eq "fork post-fork xattr isolated"     "$FORK_POST" "fork-postfork"

    teardown_two_mounts
}

# =============================================================================
# test_fork_gc_does_not_destroy_fork_data
#
# Expectation: running GC (with --delete) on the original after deleting a
# pre-fork file does not make that file unreadable on the fork.
# =============================================================================
test_fork_gc_does_not_destroy_fork_data() {
    setup_two_mounts

    # Use trash-days 0 so that rm causes refs=0 in chunk_ref immediately.
    # We then umount the original (stopping its background cleanup) before
    # running juicefs gc --delete, so GC is the sole process deciding whether
    # to delete those refs=0 slices.  GC must honour the fork lease.
    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/protected.bin bs=1M count=8 2>/dev/null
    PROTECTED_HASH=$(md5_of $MNT_ORIG/protected.bin)
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Delete the file from original — refs reach 0 in chunk_ref
    rm -f $MNT_ORIG/protected.bin
    sync

    # Umount original so its background cleanup cannot race with GC
    umount_jfs $MNT_ORIG "$META_URL"

    # GC on the original — must NOT delete fork-protected slices
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete

    # Fork must still read the file correctly
    assert_file_exists $MNT_FORK/protected.bin
    assert_eq "fork binary intact after original GC" \
        "$(md5_of $MNT_FORK/protected.bin)" "$PROTECTED_HASH"

    umount_jfs $MNT_FORK "$FORK_META_URL"
}

# =============================================================================
# test_fork_gc_cleans_own_deleted_data
#
# Expectation: GC on the fork reclaims the fork's own deleted post-fork data
# without affecting the original.
# =============================================================================
test_fork_gc_cleans_own_deleted_data() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    echo "orig baseline" > $MNT_ORIG/baseline.txt
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Fork writes then deletes its own post-fork file
    dd if=/dev/urandom of=$MNT_FORK/fork-only.bin bs=1M count=4 2>/dev/null
    sync
    rm -f $MNT_FORK/fork-only.bin
    sync

    # Umount fork so its background cleanup cannot race with GC
    umount_jfs $MNT_FORK "$FORK_META_URL"

    # GC on fork — fork's own post-fork slices (id > baseChunk) should be reclaimed
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $FORK_META_URL --delete

    # Remount fork and verify original is unaffected
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Original is completely unaffected
    assert_file_exists $MNT_ORIG/baseline.txt
    assert_eq "original baseline intact after fork GC" \
        "$(cat $MNT_ORIG/baseline.txt)" "orig baseline"

    teardown_two_mounts
}

# =============================================================================
# test_fork_multiple_forks_independent
#
# Expectation: two forks of the same original are independent of each other —
# writes on fork-A are not visible on fork-B, and vice versa.
# =============================================================================
test_fork_multiple_forks_independent() {
    FORK_META_URL_B="sqlite3:///tmp/jfs-fork-test-fork-b.db"
    MNT_FORK_B=/jfs-fork-b

    umount_jfs $MNT_ORIG    "$META_URL"          2>/dev/null || true
    umount_jfs $MNT_FORK    "$FORK_META_URL"     2>/dev/null || true
    umount_jfs $MNT_FORK_B  "$FORK_META_URL_B"   2>/dev/null || true
    python3 .github/scripts/flush_meta.py "$META_URL"
    python3 .github/scripts/flush_meta.py "$FORK_META_URL"
    python3 .github/scripts/flush_meta.py "$FORK_META_URL_B"
    flush_fork_sqlite_dbs
    rm -rf /var/jfs/myjfs         /var/jfs/myjfs-fork-a       /var/jfs/myjfs-fork-b \
           /var/jfsCache/myjfs    /var/jfsCache/myjfs-fork-a  /var/jfsCache/myjfs-fork-b \
           ~/.juicefs/local/myjfs ~/.juicefs/local/myjfs-fork-a ~/.juicefs/local/myjfs-fork-b \
           2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT_FORK $MNT_FORK_B

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    echo "shared pre-fork content" > $MNT_ORIG/shared.txt
    sync

    ./juicefs fork create $META_URL $FORK_META_URL   --name myjfs-fork-a
    ./juicefs fork create $META_URL $FORK_META_URL_B --name myjfs-fork-b
    ./juicefs mount -d $FORK_META_URL   $MNT_FORK   --no-usage-report
    ./juicefs mount -d $FORK_META_URL_B $MNT_FORK_B --no-usage-report
    sleep 1

    echo "from fork-a" > $MNT_FORK/fork-a.txt
    echo "from fork-b" > $MNT_FORK_B/fork-b.txt
    sync

    assert_file_missing $MNT_FORK/fork-b.txt
    assert_file_missing $MNT_FORK_B/fork-a.txt
    assert_file_missing $MNT_ORIG/fork-a.txt
    assert_file_missing $MNT_ORIG/fork-b.txt

    assert_eq "fork-a sees pre-fork file" \
        "$(cat $MNT_FORK/shared.txt)"   "shared pre-fork content"
    assert_eq "fork-b sees pre-fork file" \
        "$(cat $MNT_FORK_B/shared.txt)" "shared pre-fork content"

    umount_jfs $MNT_ORIG   "$META_URL"         2>/dev/null || true
    umount_jfs $MNT_FORK   "$FORK_META_URL"    2>/dev/null || true
    umount_jfs $MNT_FORK_B "$FORK_META_URL_B"  2>/dev/null || true
}

# =============================================================================
# test_fork_of_fork
#
# Expectation: forking a fork produces a third independent volume. Writes on
# fork-of-fork do not appear on its parent fork or on the original.
# =============================================================================
test_fork_of_fork() {
    FORK2_META_URL="sqlite3:///tmp/jfs-fork-test-fork2.db"
    MNT_FORK2=/jfs-fork2

    umount_jfs $MNT_ORIG  "$META_URL"       2>/dev/null || true
    umount_jfs $MNT_FORK  "$FORK_META_URL"  2>/dev/null || true
    umount_jfs $MNT_FORK2 "$FORK2_META_URL" 2>/dev/null || true
    python3 .github/scripts/flush_meta.py "$META_URL"
    python3 .github/scripts/flush_meta.py "$FORK_META_URL"
    python3 .github/scripts/flush_meta.py "$FORK2_META_URL"
    flush_fork_sqlite_dbs
    rm -rf /var/jfs/myjfs /var/jfs/myjfs-fork /var/jfs/myjfs-fork2 \
           /var/jfsCache/myjfs /var/jfsCache/myjfs-fork /var/jfsCache/myjfs-fork2 \
           ~/.juicefs/local/myjfs ~/.juicefs/local/myjfs-fork ~/.juicefs/local/myjfs-fork2 \
           2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT_FORK $MNT_FORK2

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    echo "original-data" > $MNT_ORIG/orig.txt
    sync

    # Fork original → fork1
    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1
    echo "fork1-data" > $MNT_FORK/fork1.txt
    sync

    # Fork fork1 → fork2
    ./juicefs fork create $FORK_META_URL $FORK2_META_URL --name myjfs-fork2
    ./juicefs mount -d $FORK2_META_URL $MNT_FORK2 --no-usage-report
    sleep 1

    # fork2 sees data from both original and fork1 at fork time
    assert_file_exists $MNT_FORK2/orig.txt
    assert_file_exists $MNT_FORK2/fork1.txt

    # post-fork2 write on fork2 not visible elsewhere
    echo "fork2-data" > $MNT_FORK2/fork2.txt
    sync
    assert_file_missing $MNT_ORIG/fork2.txt
    assert_file_missing $MNT_FORK/fork2.txt

    umount_jfs $MNT_ORIG  "$META_URL"       2>/dev/null || true
    umount_jfs $MNT_FORK  "$FORK_META_URL"  2>/dev/null || true
    umount_jfs $MNT_FORK2 "$FORK2_META_URL" 2>/dev/null || true
}

# =============================================================================
# test_fork_rename_isolation
#
# Expectation: renaming a pre-fork file on one volume does not affect the
# path visible on the other volume.
# =============================================================================
test_fork_rename_isolation() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    echo "rename-test" > $MNT_ORIG/before.txt
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    mv $MNT_ORIG/before.txt $MNT_ORIG/after.txt
    sync

    assert_file_missing $MNT_ORIG/before.txt
    assert_file_exists  $MNT_ORIG/after.txt
    assert_file_exists  $MNT_FORK/before.txt
    assert_file_missing $MNT_FORK/after.txt

    teardown_two_mounts
}

# =============================================================================
# test_fork_directory_isolation
#
# Expectation: mkdir on one volume post-fork does not create the directory
# on the other volume.
# =============================================================================
test_fork_directory_isolation() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    mkdir -p $MNT_ORIG/shared-dir
    echo "content" > $MNT_ORIG/shared-dir/file.txt
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    mkdir -p $MNT_ORIG/orig-new-dir
    mkdir -p $MNT_FORK/fork-new-dir
    sync

    assert_file_exists  $MNT_ORIG/shared-dir
    assert_file_exists  $MNT_FORK/shared-dir
    assert_file_exists  $MNT_ORIG/orig-new-dir
    assert_file_missing $MNT_FORK/orig-new-dir
    assert_file_missing $MNT_ORIG/fork-new-dir
    assert_file_exists  $MNT_FORK/fork-new-dir

    teardown_two_mounts
}

# =============================================================================
# test_fork_concurrent_writes_no_collision
#
# Expectation: concurrent writes of known distinct content to the same path
# on both volumes read back exactly what was written — no cross-contamination.
# Uses deterministic fill bytes so we can verify exact content, not just
# that hashes differ.
# =============================================================================
test_fork_concurrent_writes_no_collision() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Write distinct fill bytes concurrently: 0xAA to orig, 0xBB to fork
    dd if=/dev/zero bs=1M count=16 2>/dev/null | tr '\0' '\252' > $MNT_ORIG/concurrent.bin &
    PID1=$!
    dd if=/dev/zero bs=1M count=16 2>/dev/null | tr '\0' '\273' > $MNT_FORK/concurrent.bin &
    PID2=$!
    wait $PID1 $PID2
    sync

    # Verify exact first byte on each side
    ORIG_BYTE=$(xxd -p -l 1 $MNT_ORIG/concurrent.bin)
    FORK_BYTE=$(xxd -p -l 1 $MNT_FORK/concurrent.bin)
    assert_eq "original file has correct fill byte (0xaa)" "$ORIG_BYTE" "aa"
    assert_eq "fork file has correct fill byte (0xbb)"     "$FORK_BYTE" "bb"

    # Verify sizes
    assert_eq "original file size correct" "$(size_of $MNT_ORIG/concurrent.bin)" "16777216"
    assert_eq "fork file size correct"     "$(size_of $MNT_FORK/concurrent.bin)" "16777216"

    teardown_two_mounts
}

# =============================================================================
# test_fork_binary_integrity_large_file
#
# Expectation: a large pre-fork binary file reads back with identical hash
# on both volumes after several unrelated post-fork operations.
# =============================================================================
test_fork_binary_integrity_large_file() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/large.bin bs=1M count=64 2>/dev/null
    LARGE_HASH=$(md5_of $MNT_ORIG/large.bin)
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Unrelated operations on both volumes
    for i in $(seq 1 20); do
        echo "noise-$i" > $MNT_ORIG/noise-$i.txt
        echo "noise-$i" > $MNT_FORK/noise-$i.txt
    done
    dd if=/dev/urandom of=$MNT_ORIG/other.bin bs=1M count=8 2>/dev/null
    dd if=/dev/urandom of=$MNT_FORK/other.bin bs=1M count=8 2>/dev/null
    sync

    assert_eq "original large.bin hash stable after noise" \
        "$(md5_of $MNT_ORIG/large.bin)" "$LARGE_HASH"
    assert_eq "fork large.bin hash stable after noise" \
        "$(md5_of $MNT_FORK/large.bin)" "$LARGE_HASH"

    teardown_two_mounts
}

# =============================================================================
# test_fork_no_data_copy_on_create
#
# Expectation: fork completes in a time proportional to metadata size, not
# data size. A 256MB volume must fork in under 30 seconds.
# =============================================================================
test_fork_no_data_copy_on_create() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/big.bin bs=1M count=256 2>/dev/null
    sync

    FORK_START=$(date +%s)
    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    FORK_END=$(date +%s)
    FORK_ELAPSED=$((FORK_END - FORK_START))

    if [[ $FORK_ELAPSED -gt 30 ]]; then
        echo "<FATAL> fork took ${FORK_ELAPSED}s — expected metadata-only speed (<30s)"
        exit 1
    fi
    echo "fork completed in ${FORK_ELAPSED}s (expected <30s) — OK"

    teardown_two_mounts
}

# =============================================================================
# test_fork_idempotent_meta_url
#
# Expectation: forking into an already-populated dst-meta fails with a clear
# error (same guard as juicefs load).
# =============================================================================
test_fork_idempotent_meta_url() {
    setup_two_mounts

    ./juicefs format $META_URL      myjfs
    ./juicefs format $FORK_META_URL myjfs-fork

    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    echo "data" > $MNT_ORIG/file.txt
    sync

    set +e
    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork-2
    EXIT_CODE=$?
    set -e

    if [[ $EXIT_CODE -eq 0 ]]; then
        echo "<FATAL> fork into non-empty dst-meta should have failed but succeeded"
        exit 1
    fi
    echo "correctly rejected fork into non-empty dst-meta (exit $EXIT_CODE)"

    teardown_two_mounts
}

# =============================================================================
# test_fork_list_shows_active_forks
#
# Expectation: juicefs fork list shows all active forks of a source volume.
# =============================================================================
test_fork_list_shows_active_forks() {
    FORK_META_URL_B="sqlite3:///tmp/jfs-fork-test-list-b.db"

    umount_jfs $MNT_ORIG "$META_URL"      2>/dev/null || true
    umount_jfs $MNT_FORK "$FORK_META_URL" 2>/dev/null || true
    python3 .github/scripts/flush_meta.py "$META_URL"
    python3 .github/scripts/flush_meta.py "$FORK_META_URL"
    python3 .github/scripts/flush_meta.py "$FORK_META_URL_B"
    flush_fork_sqlite_dbs
    rm -rf /var/jfs/myjfs /var/jfs/myjfs-fork-list-a /var/jfs/myjfs-fork-list-b \
           ~/.juicefs/local/myjfs ~/.juicefs/local/myjfs-fork-list-a \
           ~/.juicefs/local/myjfs-fork-list-b \
           2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT_FORK

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    ./juicefs fork create $META_URL $FORK_META_URL   --name myjfs-fork-list-a
    ./juicefs fork create $META_URL $FORK_META_URL_B --name myjfs-fork-list-b

    FORK_LIST=$(./juicefs fork list $META_URL)
    echo "$FORK_LIST" | grep -q "myjfs-fork-list-a" || {
        echo "<FATAL> fork list does not contain myjfs-fork-list-a"
        exit 1
    }
    echo "$FORK_LIST" | grep -q "myjfs-fork-list-b" || {
        echo "<FATAL> fork list does not contain myjfs-fork-list-b"
        exit 1
    }

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    python3 .github/scripts/flush_meta.py "$FORK_META_URL"
    python3 .github/scripts/flush_meta.py "$FORK_META_URL_B"
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_destroy_releases_gc
#
# Expectation: after destroying a fork and releasing its lease, GC on the
# original is able to reclaim objects deleted from the original.
# Also verifies the object count in the bucket decreases after GC runs,
# confirming reclaim actually happened (not just lease removal).
# =============================================================================
test_fork_destroy_releases_gc() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/reclaimable.bin bs=1M count=8 2>/dev/null
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Delete from original — GC should be blocked by lease
    rm -f $MNT_ORIG/reclaimable.bin
    sync
    umount_jfs $MNT_ORIG "$META_URL"
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete

    # Fork still has it — lease protected the chunks
    assert_file_exists $MNT_FORK/reclaimable.bin

    # Count objects before lease release (must be > 0: chunks still present)
    OBJECTS_BEFORE=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")
    if [[ "$OBJECTS_BEFORE" -eq 0 ]]; then
        echo "<FATAL> expected chunks to still exist before lease release, got $OBJECTS_BEFORE objects"
        exit 1
    fi

    # Release lease first (before wiping the fork's metadata DB)
    umount_jfs $MNT_FORK "$FORK_META_URL" 2>/dev/null || true
    ./juicefs fork release $META_URL --fork-name myjfs-fork
    python3 .github/scripts/flush_meta.py "$FORK_META_URL"
    flush_fork_sqlite_dbs

    # Verify lease is gone
    FORK_LIST=$(./juicefs fork list $META_URL 2>&1)
    if echo "$FORK_LIST" | grep -q "myjfs-fork"; then
        echo "<FATAL> lease still present after fork release"
        exit 1
    fi

    # GC should now reclaim the chunks (no lease blocking it)
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete
    OBJECTS_AFTER=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")

    if [[ "$OBJECTS_AFTER" -ge "$OBJECTS_BEFORE" ]]; then
        echo "<FATAL> object count did not decrease after GC post-lease-release (before=$OBJECTS_BEFORE after=$OBJECTS_AFTER)"
        exit 1
    fi
    echo "GC reclaimed objects after lease release (before=$OBJECTS_BEFORE after=$OBJECTS_AFTER) — OK"

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    umount_jfs $MNT_FORK "$FORK_META_URL" 2>/dev/null || true
}

# =============================================================================
# skip_test_fork_live_source
#
# Documents expected behaviour when forking a mounted, actively-written volume.
# This is a non-guarantee (fuzzy snapshot) per the design doc.
# Skipped by default — rename to test_* to run explicitly.
#
# Expectation: fork succeeds without crashing. The fork contains a consistent
# subset of writes (no torn files, no metadata corruption), but is NOT required
# to capture all in-flight writes at the exact fork instant.
# =============================================================================
skip_test_fork_live_source() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    # Start a background writer
    (
        for i in $(seq 1 100); do
            echo "write-$i" > $MNT_ORIG/live-$i.txt
            sleep 0.05
        done
    ) &
    WRITER_PID=$!

    sleep 1  # Let writer get some files in

    # Fork while writer is active
    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    FORK_EXIT=$?

    wait $WRITER_PID || true

    if [[ $FORK_EXIT -ne 0 ]]; then
        echo "<FATAL> fork failed on live source (exit $FORK_EXIT)"
        exit 1
    fi

    # Fork must be mountable and metadata-consistent (no fsck errors)
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1
    ./juicefs fsck $FORK_META_URL
    echo "fork of live source succeeded and passed fsck — OK (fuzzy snapshot accepted)"

    teardown_two_mounts
}

# =============================================================================
# test_fork_destroy_protection
#
# Expectation:
#   1. `juicefs destroy` on a source volume with active fork leases is refused
#      (exits non-zero) to prevent corrupting the forks.
#   2. `juicefs destroy` on a shared-storage fork only wipes the fork's metadata
#      DB; the shared objects in the bucket remain intact so the source and any
#      remaining sibling forks are unaffected.
# =============================================================================
test_fork_destroy_protection() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/shared.bin bs=1M count=4 2>/dev/null
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork

    # --- Guard 1: destroy source while fork lease is active must be refused ---
    # Get fork UUID from the fork's own metadata
    FORK_UUID=$(./juicefs status $FORK_META_URL 2>&1 | grep -oP '"UUID":\s*"[^"]+"' | grep -oP '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)
    SRC_UUID=$(./juicefs status $META_URL 2>&1 | grep -oP '"UUID":\s*"[^"]+"' | grep -oP '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}' | head -1)

    umount_jfs $MNT_ORIG "$META_URL"

    if ./juicefs destroy --yes "$META_URL" "$SRC_UUID" 2>&1; then
        echo "<FATAL> destroy source with active fork lease should have been refused"
        exit 1
    fi
    echo "destroy source with active lease was refused — OK"

    # Source objects must still be intact
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    assert_file_exists $MNT_ORIG/shared.bin
    umount_jfs $MNT_ORIG "$META_URL"

    # --- Guard 2: destroy fork only wipes metadata, not shared objects ---
    # Count objects before fork destroy
    OBJS_BEFORE=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")

    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1
    assert_file_exists $MNT_FORK/shared.bin
    umount_jfs $MNT_FORK "$FORK_META_URL"

    # Release the lease first (normal workflow before destroying a fork)
    ./juicefs fork release $META_URL --fork-name myjfs-fork

    # Now destroy the fork — metadata only, objects untouched
    if ! ./juicefs destroy --yes "$FORK_META_URL" "$FORK_UUID" 2>&1; then
        echo "<FATAL> destroy fork (after lease release) should have succeeded"
        exit 1
    fi
    echo "destroy fork (metadata-only) succeeded — OK"

    # Source objects must still be intact after fork destroy
    OBJS_AFTER=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")
    if [[ "$OBJS_AFTER" -lt "$OBJS_BEFORE" ]]; then
        echo "<FATAL> object count dropped after fork destroy (before=$OBJS_BEFORE after=$OBJS_AFTER) — shared objects were deleted!"
        exit 1
    fi
    echo "object count stable after fork destroy (before=$OBJS_BEFORE after=$OBJS_AFTER) — OK"

    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    assert_file_exists $MNT_ORIG/shared.bin
    echo "source file still readable after fork destroy — OK"

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_gc_does_not_destroy_fork_data_via_fork_gc
#
# Checkpoint/restore scenario: the fork IS the checkpoint.
# Equal-privilege: GC run on the FORK itself must not delete pre-fork shared
# chunks even when the fork user deletes those files from the fork.
# (This validates the forkProtectBelow counter written into the fork's own DB.)
# =============================================================================
test_fork_gc_does_not_destroy_fork_data_via_fork_gc() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/checkpoint.bin bs=1M count=8 2>/dev/null
    CHECKPOINT_HASH=$(md5_of $MNT_ORIG/checkpoint.bin)
    sync

    # Fork = take a checkpoint
    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Delete the pre-fork file from the FORK (simulating: checkpoint holder cleans up)
    rm -f $MNT_FORK/checkpoint.bin
    sync
    umount_jfs $MNT_FORK "$FORK_META_URL"

    # GC on the FORK — must NOT delete the shared pre-fork chunk
    # (the source still references it through its own metadata)
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $FORK_META_URL --delete

    # Source must still read the file correctly — the chunk was protected
    assert_file_exists $MNT_ORIG/checkpoint.bin
    assert_eq "source checkpoint intact after fork GC" \
        "$(md5_of $MNT_ORIG/checkpoint.bin)" "$CHECKPOINT_HASH"
    echo "pre-fork chunk survived fork's own GC — OK"

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_multi_checkpoint_gc_blocked_until_all_released
#
# Checkpoint/restore scenario: two checkpoints (forks) of the same source.
# GC on the source must be blocked as long as ANY checkpoint still holds a
# lease. Releasing only one checkpoint is not enough — the other still
# references the pre-fork data.
# =============================================================================
test_fork_multi_checkpoint_gc_blocked_until_all_released() {
    FORK_META_URL_B="sqlite3:///tmp/jfs-fork-test-fork-b.db"
    MNT_FORK_B=/jfs-fork-b

    umount_jfs $MNT_ORIG   "$META_URL"        2>/dev/null || true
    umount_jfs $MNT_FORK   "$FORK_META_URL"   2>/dev/null || true
    umount_jfs $MNT_FORK_B "$FORK_META_URL_B" 2>/dev/null || true
    python3 .github/scripts/flush_meta.py "$META_URL"
    python3 .github/scripts/flush_meta.py "$FORK_META_URL"
    python3 .github/scripts/flush_meta.py "$FORK_META_URL_B"
    flush_fork_sqlite_dbs
    rm -f /tmp/jfs-fork-test-fork-b.db 2>/dev/null || true
    rm -rf /var/jfs/myjfs /var/jfsCache/myjfs 2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT_FORK $MNT_FORK_B

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/shared.bin bs=1M count=4 2>/dev/null
    SHARED_HASH=$(md5_of $MNT_ORIG/shared.bin)
    sync

    # Two checkpoints
    ./juicefs fork create $META_URL $FORK_META_URL   --name ckpt-a
    ./juicefs fork create $META_URL $FORK_META_URL_B --name ckpt-b
    ./juicefs mount -d $FORK_META_URL   $MNT_FORK   --no-usage-report
    ./juicefs mount -d $FORK_META_URL_B $MNT_FORK_B --no-usage-report
    sleep 1

    # Delete from source — GC should be blocked by both leases
    rm -f $MNT_ORIG/shared.bin
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete

    # Both checkpoints still readable
    assert_file_exists $MNT_FORK/shared.bin
    assert_file_exists $MNT_FORK_B/shared.bin
    assert_eq "ckpt-a intact after GC (both leases)" \
        "$(md5_of $MNT_FORK/shared.bin)" "$SHARED_HASH"
    assert_eq "ckpt-b intact after GC (both leases)" \
        "$(md5_of $MNT_FORK_B/shared.bin)" "$SHARED_HASH"
    echo "both checkpoints intact when both leases active — OK"

    # Release ONE checkpoint lease — GC still blocked by the remaining one
    umount_jfs $MNT_FORK "$FORK_META_URL"
    ./juicefs fork release $META_URL --fork-name ckpt-a
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete

    # Remaining checkpoint must still be readable
    assert_file_exists $MNT_FORK_B/shared.bin
    assert_eq "ckpt-b intact after GC (one lease released)" \
        "$(md5_of $MNT_FORK_B/shared.bin)" "$SHARED_HASH"
    echo "remaining checkpoint intact after partial lease release — OK"

    # Release the LAST lease — now GC can reclaim
    umount_jfs $MNT_FORK_B "$FORK_META_URL_B"
    ./juicefs fork release $META_URL --fork-name ckpt-b

    OBJS_BEFORE=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete
    OBJS_AFTER=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")

    if [[ "$OBJS_AFTER" -ge "$OBJS_BEFORE" ]]; then
        echo "<FATAL> expected GC to reclaim after all leases released (before=$OBJS_BEFORE after=$OBJS_AFTER)"
        exit 1
    fi
    echo "GC reclaimed after all leases released (before=$OBJS_BEFORE after=$OBJS_AFTER) — OK"

    umount_jfs $MNT_ORIG   "$META_URL"        2>/dev/null || true
    umount_jfs $MNT_FORK   "$FORK_META_URL"   2>/dev/null || true
    umount_jfs $MNT_FORK_B "$FORK_META_URL_B" 2>/dev/null || true
    rm -f /tmp/jfs-fork-test-fork-b.db 2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_restore_workflow
#
# Checkpoint/restore scenario: the fork IS the restore target.
# User takes a checkpoint (fork), corrupts the source, then "restores" by
# using the checkpoint fork as the new operational volume. The restored volume
# must contain exactly the checkpoint-time data, readable and writable.
# =============================================================================
test_fork_restore_workflow() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    # Write baseline data — this is the state we want to "restore" to
    echo "v1 data" > $MNT_ORIG/important.txt
    dd if=/dev/urandom of=$MNT_ORIG/bigfile.bin bs=1M count=4 2>/dev/null
    BASELINE_HASH=$(md5_of $MNT_ORIG/bigfile.bin)
    sync

    # Take a checkpoint (fork)
    ./juicefs fork create $META_URL $FORK_META_URL --name checkpoint-v1

    # "Corrupt" the source by overwriting and adding garbage
    echo "corrupted" > $MNT_ORIG/important.txt
    dd if=/dev/zero of=$MNT_ORIG/bigfile.bin bs=1M count=4 2>/dev/null
    echo "junk" > $MNT_ORIG/junk.txt
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    # "Restore": mount the checkpoint fork as the operational volume
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Checkpoint must reflect pre-corruption state exactly
    assert_file_exists $MNT_FORK/important.txt
    assert_eq "restored important.txt" "$(cat $MNT_FORK/important.txt)" "v1 data"
    assert_file_exists $MNT_FORK/bigfile.bin
    assert_eq "restored bigfile hash" "$(md5_of $MNT_FORK/bigfile.bin)" "$BASELINE_HASH"
    assert_file_missing $MNT_FORK/junk.txt
    echo "restore from checkpoint reflects pre-corruption state — OK"

    # Restored volume is writable (not read-only)
    echo "post-restore write" > $MNT_FORK/new-after-restore.txt
    sync
    assert_eq "post-restore write" "$(cat $MNT_FORK/new-after-restore.txt)" "post-restore write"
    echo "checkpoint mount is writable after restore — OK"

    umount_jfs $MNT_FORK "$FORK_META_URL" 2>/dev/null || true
    umount_jfs $MNT_ORIG "$META_URL"      2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_protection_survives_source_restart
#
# Checkpoint/restore scenario: fork protection must survive the source volume
# being unmounted and remounted (process restart). The forkProtectBelow counter
# is persisted in the metadata DB so a fresh mount picks it up and still
# protects the checkpoint data.
# =============================================================================
test_fork_protection_survives_source_restart() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/checkpoint.bin bs=1M count=4 2>/dev/null
    CHECKPOINT_HASH=$(md5_of $MNT_ORIG/checkpoint.bin)
    sync

    # Take checkpoint (fork)
    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Delete from source, then fully stop and restart the source mount
    rm -f $MNT_ORIG/checkpoint.bin
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    # Source process is dead — simulate restart
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    # Immediately run GC — the fresh mount must have loaded forkProtectBelow
    # from the persistent DB counter, not just in-memory state
    umount_jfs $MNT_ORIG "$META_URL"
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete

    # Checkpoint (fork) must still be intact
    assert_file_exists $MNT_FORK/checkpoint.bin
    assert_eq "checkpoint intact after source restart + GC" \
        "$(md5_of $MNT_FORK/checkpoint.bin)" "$CHECKPOINT_HASH"
    echo "fork protection survived source restart — OK"

    umount_jfs $MNT_FORK "$FORK_META_URL" 2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_pre_fork_data_survives_compaction
#
# Checkpoint/restore scenario: compaction of small slices on the source must
# not delete or corrupt pre-fork chunks that the checkpoint fork still reads.
# Compaction rewrites slices; the old slice IDs (below forkBaseChunk) must be
# deleted only after all forks release their leases.
# =============================================================================
test_fork_pre_fork_data_survives_compaction() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    # Write many small appends to create multiple slices for the same chunk
    # (this is the typical pattern that triggers compaction)
    for i in $(seq 1 16); do
        dd if=/dev/urandom bs=256K count=1 >> $MNT_ORIG/fragmented.bin 2>/dev/null
    done
    BEFORE_HASH=$(md5_of $MNT_ORIG/fragmented.bin)
    sync

    # Take a checkpoint before compaction
    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    FORK_HASH=$(md5_of $MNT_FORK/fragmented.bin)
    assert_eq "fork hash matches source before compaction" "$FORK_HASH" "$BEFORE_HASH"

    # Run compaction on the source — rewrites slices, marks old ones as pending delete
    umount_jfs $MNT_ORIG "$META_URL"
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --compact --delete

    # Checkpoint fork must still read the file with the same content
    assert_file_exists $MNT_FORK/fragmented.bin
    assert_eq "fork hash unchanged after source compaction" \
        "$(md5_of $MNT_FORK/fragmented.bin)" "$BEFORE_HASH"
    echo "pre-fork data readable on fork after source compaction — OK"

    # Source also remounts correctly post-compaction
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    assert_file_exists $MNT_ORIG/fragmented.bin
    assert_eq "source hash unchanged after compaction" \
        "$(md5_of $MNT_ORIG/fragmented.bin)" "$BEFORE_HASH"
    echo "source data consistent after compaction — OK"

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    umount_jfs $MNT_FORK "$FORK_META_URL" 2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_source_delete_then_checkpoint_survives
#
# Scenario (the most important regression chain):
#
#   1. Write data on source (v0 baseline)
#   2. Fork source → fork-a (checkpoint of source at v0)
#   3. Fork source → fork-b (another checkpoint of source at v0)
#   4. Write diverged data on fork-a and fork-b independently
#   5. Fork fork-a → ckpt-a  (checkpoint of fork-a)
#   6. Fork fork-b → ckpt-b  (checkpoint of fork-b)
#   7. Delete data from fork-a and fork-b (including pre-fork shared files)
#   8. Run GC on fork-a and fork-b with --delete
#   9. ckpt-a must still contain fork-a's pre-deletion state intact
#  10. ckpt-b must still contain fork-b's pre-deletion state intact
#  11. Destroy source (release leases first) — forks and checkpoints unaffected
#
# This catches:
#   - Fork-of-fork losing its grandparent pre-fork chunks after GC on parent
#   - Cross-sibling: fork-a GC deleting chunks fork-b and ckpt-b still need
#   - Source destruction cascading to forks/checkpoints through shared objects
#   - Layered forkProtectBelow: ckpt-a inherits TWO protection tiers
# =============================================================================
test_fork_source_delete_then_checkpoint_survives() {
    FORK_META_A="sqlite3:///tmp/jfs-ckpt-fork-a.db"
    FORK_META_B="sqlite3:///tmp/jfs-ckpt-fork-b.db"
    CKPT_META_A="sqlite3:///tmp/jfs-ckpt-ckpt-a.db"
    CKPT_META_B="sqlite3:///tmp/jfs-ckpt-ckpt-b.db"
    MNT_A=/jfs-ckpt-a
    MNT_B=/jfs-ckpt-b
    MNT_CA=/jfs-ckpt-ca
    MNT_CB=/jfs-ckpt-cb

    # Full teardown
    umount_jfs $MNT_ORIG "$META_URL"    2>/dev/null || true
    umount_jfs $MNT_A    "$FORK_META_A" 2>/dev/null || true
    umount_jfs $MNT_B    "$FORK_META_B" 2>/dev/null || true
    umount_jfs $MNT_CA   "$CKPT_META_A" 2>/dev/null || true
    umount_jfs $MNT_CB   "$CKPT_META_B" 2>/dev/null || true
    for db in "$META_URL" "$FORK_META_A" "$FORK_META_B" "$CKPT_META_A" "$CKPT_META_B"; do
        python3 .github/scripts/flush_meta.py "$db" 2>/dev/null || true
    done
    rm -f /tmp/jfs-ckpt-fork-a.db /tmp/jfs-ckpt-fork-b.db \
          /tmp/jfs-ckpt-ckpt-a.db /tmp/jfs-ckpt-ckpt-b.db 2>/dev/null || true
    rm -rf /var/jfs/myjfs /var/jfsCache/myjfs 2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT_A $MNT_B $MNT_CA $MNT_CB

    # --- Step 1: Write baseline data on source ---
    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/v0-shared.bin bs=1M count=4 2>/dev/null
    echo "v0 text" > $MNT_ORIG/v0-text.txt
    V0_BIN_HASH=$(md5_of $MNT_ORIG/v0-shared.bin)
    sync

    # --- Step 2+3: Fork source → fork-a and fork-b (both checkpoints of v0) ---
    ./juicefs fork create $META_URL $FORK_META_A --name fork-a
    ./juicefs fork create $META_URL $FORK_META_B --name fork-b

    ./juicefs mount -d $FORK_META_A $MNT_A --no-usage-report
    ./juicefs mount -d $FORK_META_B $MNT_B --no-usage-report
    sleep 1

    # Sanity: both forks see source v0 data
    assert_eq "fork-a sees v0 text" "$(cat $MNT_A/v0-text.txt)" "v0 text"
    assert_eq "fork-b sees v0 text" "$(cat $MNT_B/v0-text.txt)" "v0 text"

    # --- Step 4: Write diverged data on each fork ---
    dd if=/dev/urandom of=$MNT_A/fork-a-own.bin bs=1M count=2 2>/dev/null
    echo "fork-a extra" > $MNT_A/fork-a-extra.txt
    FORK_A_OWN_HASH=$(md5_of $MNT_A/fork-a-own.bin)

    dd if=/dev/urandom of=$MNT_B/fork-b-own.bin bs=1M count=2 2>/dev/null
    echo "fork-b extra" > $MNT_B/fork-b-extra.txt
    FORK_B_OWN_HASH=$(md5_of $MNT_B/fork-b-own.bin)
    sync

    # --- Step 5+6: Checkpoint each fork ---
    ./juicefs fork create $FORK_META_A $CKPT_META_A --name ckpt-a
    ./juicefs fork create $FORK_META_B $CKPT_META_B --name ckpt-b

    ./juicefs mount -d $CKPT_META_A $MNT_CA --no-usage-report
    ./juicefs mount -d $CKPT_META_B $MNT_CB --no-usage-report
    sleep 1

    # Sanity: checkpoints see full lineage
    assert_file_exists $MNT_CA/v0-shared.bin
    assert_file_exists $MNT_CA/fork-a-own.bin
    assert_file_exists $MNT_CB/v0-shared.bin
    assert_file_exists $MNT_CB/fork-b-own.bin
    assert_eq "ckpt-a sees v0 bin" "$(md5_of $MNT_CA/v0-shared.bin)" "$V0_BIN_HASH"
    assert_eq "ckpt-b sees v0 bin" "$(md5_of $MNT_CB/v0-shared.bin)" "$V0_BIN_HASH"
    echo "checkpoints see full lineage before deletions — OK"

    # --- Step 7: Delete everything from fork-a and fork-b ---
    rm -f $MNT_A/v0-shared.bin $MNT_A/v0-text.txt $MNT_A/fork-a-own.bin $MNT_A/fork-a-extra.txt
    rm -f $MNT_B/v0-shared.bin $MNT_B/v0-text.txt $MNT_B/fork-b-own.bin $MNT_B/fork-b-extra.txt
    sync

    umount_jfs $MNT_A "$FORK_META_A"
    umount_jfs $MNT_B "$FORK_META_B"

    # --- Step 8: GC on fork-a and fork-b with --delete ---
    # fork-a GC: must not delete v0-shared.bin chunks (ckpt-a and fork-b/ckpt-b need them)
    #            must not delete fork-a-own.bin chunks (ckpt-a needs them)
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $FORK_META_A --delete
    # fork-b GC: same constraints for its lineage
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $FORK_META_B --delete

    # --- Step 9+10: Checkpoints must still contain full pre-deletion state ---
    assert_file_exists $MNT_CA/v0-shared.bin
    assert_file_exists $MNT_CA/fork-a-own.bin
    assert_file_exists $MNT_CA/v0-text.txt
    assert_file_exists $MNT_CA/fork-a-extra.txt
    assert_eq "ckpt-a v0-shared.bin intact after fork-a GC" \
        "$(md5_of $MNT_CA/v0-shared.bin)" "$V0_BIN_HASH"
    assert_eq "ckpt-a fork-a-own.bin intact after fork-a GC" \
        "$(md5_of $MNT_CA/fork-a-own.bin)" "$FORK_A_OWN_HASH"
    echo "ckpt-a fully intact after fork-a GC — OK"

    assert_file_exists $MNT_CB/v0-shared.bin
    assert_file_exists $MNT_CB/fork-b-own.bin
    assert_file_exists $MNT_CB/v0-text.txt
    assert_file_exists $MNT_CB/fork-b-extra.txt
    assert_eq "ckpt-b v0-shared.bin intact after fork-b GC" \
        "$(md5_of $MNT_CB/v0-shared.bin)" "$V0_BIN_HASH"
    assert_eq "ckpt-b fork-b-own.bin intact after fork-b GC" \
        "$(md5_of $MNT_CB/fork-b-own.bin)" "$FORK_B_OWN_HASH"
    echo "ckpt-b fully intact after fork-b GC — OK"

    # Cross-check: fork-a GC did not delete chunks that fork-b (sibling) needs
    # (v0-shared.bin exists in ckpt-b, which descends from fork-b, which descends from source)
    assert_eq "ckpt-b v0 bin unaffected by fork-a GC" \
        "$(md5_of $MNT_CB/v0-shared.bin)" "$V0_BIN_HASH"
    echo "cross-sibling GC isolation — OK"

    # --- Step 11: Destroy source after releasing its leases ---
    # Source still has active leases for fork-a and fork-b
    # Releasing them from source allows source to be destroyed safely.
    # But fork-a and fork-b themselves still have ckpt-a / ckpt-b leases —
    # those are on fork-a/fork-b as source, not on the original source.
    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    ./juicefs fork release $META_URL --fork-name fork-a
    ./juicefs fork release $META_URL --fork-name fork-b

    # Now source can be destroyed (only wipes source metadata, chunks still shared)
    SRC_UUID=$(./juicefs status $META_URL 2>&1 | grep -oP '"UUID":\s*"[^"]+"' | grep -oP '[0-9a-f-]{36}' | head -1)
    if [[ -n "$SRC_UUID" ]]; then
        ./juicefs destroy --yes "$META_URL" "$SRC_UUID" 2>&1 || true
        echo "source destroyed — OK"
    fi

    # Checkpoints must still be intact after source destruction
    assert_file_exists $MNT_CA/v0-shared.bin
    assert_file_exists $MNT_CB/v0-shared.bin
    assert_eq "ckpt-a v0 bin intact after source destroyed" \
        "$(md5_of $MNT_CA/v0-shared.bin)" "$V0_BIN_HASH"
    assert_eq "ckpt-b v0 bin intact after source destroyed" \
        "$(md5_of $MNT_CB/v0-shared.bin)" "$V0_BIN_HASH"
    echo "checkpoints intact after source destruction — OK"

    # Cleanup
    umount_jfs $MNT_CA "$CKPT_META_A" 2>/dev/null || true
    umount_jfs $MNT_CB "$CKPT_META_B" 2>/dev/null || true
    rm -f /tmp/jfs-ckpt-fork-a.db /tmp/jfs-ckpt-fork-b.db \
          /tmp/jfs-ckpt-ckpt-a.db /tmp/jfs-ckpt-ckpt-b.db 2>/dev/null || true
}

# =============================================================================
# test_fork_sibling_gc_isolation
#
# Scenario: two sibling forks (fork-a, fork-b) from the same source both hold
# pre-fork chunks in the shared namespace. Running GC --delete on fork-a must
# not delete chunks that fork-b still needs, even after fork-a has deleted
# those files from its own metadata.
#
# This specifically catches the case where forkProtectBelow is only written
# in the fork's own DB but another sibling relies on the same chunks.
# (The bucket lease is the authoritative guard here — GC reads it from the
# bucket and installs the minimum protection threshold across all active leases.)
# =============================================================================
test_fork_sibling_gc_isolation() {
    FORK_META_A="sqlite3:///tmp/jfs-sibling-fork-a.db"
    FORK_META_B="sqlite3:///tmp/jfs-sibling-fork-b.db"
    MNT_A=/jfs-sibling-a
    MNT_B=/jfs-sibling-b

    umount_jfs $MNT_ORIG "$META_URL"    2>/dev/null || true
    umount_jfs $MNT_A    "$FORK_META_A" 2>/dev/null || true
    umount_jfs $MNT_B    "$FORK_META_B" 2>/dev/null || true
    for db in "$META_URL" "$FORK_META_A" "$FORK_META_B"; do
        python3 .github/scripts/flush_meta.py "$db" 2>/dev/null || true
    done
    rm -f /tmp/jfs-sibling-fork-a.db /tmp/jfs-sibling-fork-b.db 2>/dev/null || true
    rm -rf /var/jfs/myjfs /var/jfsCache/myjfs 2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT_A $MNT_B

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    # Write a file that will be shared by both forks
    dd if=/dev/urandom of=$MNT_ORIG/sibling-shared.bin bs=1M count=4 2>/dev/null
    SHARED_HASH=$(md5_of $MNT_ORIG/sibling-shared.bin)
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    # Both forks inherit the same pre-fork chunk
    ./juicefs fork create $META_URL $FORK_META_A --name fork-a
    ./juicefs fork create $META_URL $FORK_META_B --name fork-b
    ./juicefs mount -d $FORK_META_A $MNT_A --no-usage-report
    ./juicefs mount -d $FORK_META_B $MNT_B --no-usage-report
    sleep 1

    # fork-a deletes the shared file; fork-b keeps it
    rm -f $MNT_A/sibling-shared.bin
    sync
    umount_jfs $MNT_A "$FORK_META_A"

    # GC on fork-a: the pre-fork chunk has refs=0 in fork-a's metadata,
    # but fork-b's lease is still active — the bucket lease must protect it.
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $FORK_META_A --delete

    # fork-b must still be able to read the file
    assert_file_exists $MNT_B/sibling-shared.bin
    assert_eq "fork-b sees shared file after fork-a GC" \
        "$(md5_of $MNT_B/sibling-shared.bin)" "$SHARED_HASH"
    echo "sibling fork-b unaffected by fork-a GC — OK"

    # Source also still has the chunks referenced (it hasn't deleted the file)
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    assert_file_exists $MNT_ORIG/sibling-shared.bin
    assert_eq "source sees shared file after fork-a GC" \
        "$(md5_of $MNT_ORIG/sibling-shared.bin)" "$SHARED_HASH"
    echo "source unaffected by fork-a GC — OK"

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    umount_jfs $MNT_B "$FORK_META_B" 2>/dev/null || true
    rm -f /tmp/jfs-sibling-fork-a.db /tmp/jfs-sibling-fork-b.db 2>/dev/null || true
}

# =============================================================================
# test_fork_checkpoint_of_checkpoint_gc_chain
#
# Scenario: three-level chain — source → fork1 → fork2 (checkpoint of checkpoint).
# Data written at each level, then the middle level (fork1) deletes its files
# and runs GC. The deepest checkpoint (fork2) must still see all data from
# both source and fork1 levels. The source must be unaffected.
#
# Catches: multi-hop forkProtectBelow (fork2 has two protection tiers —
# one from source-fork1 and one from fork1-fork2 boundaries).
# =============================================================================
test_fork_checkpoint_of_checkpoint_gc_chain() {
    FORK1_META="sqlite3:///tmp/jfs-chain-fork1.db"
    FORK2_META="sqlite3:///tmp/jfs-chain-fork2.db"
    MNT1=/jfs-chain-1
    MNT2=/jfs-chain-2

    umount_jfs $MNT_ORIG "$META_URL"   2>/dev/null || true
    umount_jfs $MNT1     "$FORK1_META" 2>/dev/null || true
    umount_jfs $MNT2     "$FORK2_META" 2>/dev/null || true
    for db in "$META_URL" "$FORK1_META" "$FORK2_META"; do
        python3 .github/scripts/flush_meta.py "$db" 2>/dev/null || true
    done
    rm -f /tmp/jfs-chain-fork1.db /tmp/jfs-chain-fork2.db 2>/dev/null || true
    rm -rf /var/jfs/myjfs /var/jfsCache/myjfs 2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT1 $MNT2

    # Level 0: source with v0 data
    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    dd if=/dev/urandom of=$MNT_ORIG/level0.bin bs=1M count=2 2>/dev/null
    L0_HASH=$(md5_of $MNT_ORIG/level0.bin)
    sync

    # Level 1: fork source → fork1, add fork1-level data
    ./juicefs fork create $META_URL $FORK1_META --name fork1
    ./juicefs mount -d $FORK1_META $MNT1 --no-usage-report
    sleep 1
    dd if=/dev/urandom of=$MNT1/level1.bin bs=1M count=2 2>/dev/null
    L1_HASH=$(md5_of $MNT1/level1.bin)
    sync

    # Level 2: fork fork1 → fork2 (checkpoint of the checkpoint)
    ./juicefs fork create $FORK1_META $FORK2_META --name fork2
    ./juicefs mount -d $FORK2_META $MNT2 --no-usage-report
    sleep 1

    # fork2 must see both levels
    assert_file_exists $MNT2/level0.bin
    assert_file_exists $MNT2/level1.bin
    assert_eq "fork2 sees level0" "$(md5_of $MNT2/level0.bin)" "$L0_HASH"
    assert_eq "fork2 sees level1" "$(md5_of $MNT2/level1.bin)" "$L1_HASH"

    # Delete everything from fork1 (middle level) and run GC
    rm -f $MNT1/level0.bin $MNT1/level1.bin
    sync
    umount_jfs $MNT1 "$FORK1_META"
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $FORK1_META --delete

    # fork2 (deepest checkpoint) must still see both levels intact
    assert_file_exists $MNT2/level0.bin
    assert_file_exists $MNT2/level1.bin
    assert_eq "fork2 level0 intact after fork1 GC" "$(md5_of $MNT2/level0.bin)" "$L0_HASH"
    assert_eq "fork2 level1 intact after fork1 GC" "$(md5_of $MNT2/level1.bin)" "$L1_HASH"
    echo "3-level checkpoint chain intact after middle-level GC — OK"

    # Source must also be unaffected
    assert_file_exists $MNT_ORIG/level0.bin
    assert_eq "source level0 intact after fork1 GC" "$(md5_of $MNT_ORIG/level0.bin)" "$L0_HASH"
    echo "source unaffected by fork1 GC — OK"

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    umount_jfs $MNT2 "$FORK2_META"  2>/dev/null || true
    rm -f /tmp/jfs-chain-fork1.db /tmp/jfs-chain-fork2.db 2>/dev/null || true
}

# =============================================================================
# test_fork_recreate_after_full_release
#
# Adversarial: Bug #4 regression.
# Full release cycle (A+B released → forkProtectCleared=1) followed by a NEW
# fork C.  The cleared flag must be overridden by the rearm counter so that C's
# pre-fork data is NOT deleted by GC.
# =============================================================================
test_fork_recreate_after_full_release() {
    FORK_META_A="sqlite3:///tmp/jfs-recreate-a.db"
    FORK_META_B="sqlite3:///tmp/jfs-recreate-b.db"
    FORK_META_C="sqlite3:///tmp/jfs-recreate-c.db"
    MNT_A=/jfs-recreate-a
    MNT_B=/jfs-recreate-b
    MNT_C=/jfs-recreate-c

    umount_jfs $MNT_ORIG "$META_URL"    2>/dev/null || true
    umount_jfs $MNT_A    "$FORK_META_A" 2>/dev/null || true
    umount_jfs $MNT_B    "$FORK_META_B" 2>/dev/null || true
    umount_jfs $MNT_C    "$FORK_META_C" 2>/dev/null || true
    for db in "$META_URL" "$FORK_META_A" "$FORK_META_B" "$FORK_META_C"; do
        python3 .github/scripts/flush_meta.py "$db" 2>/dev/null || true
    done
    rm -f /tmp/jfs-recreate-a.db /tmp/jfs-recreate-b.db \
          /tmp/jfs-recreate-c.db 2>/dev/null || true
    rm -rf /var/jfs/myjfs /var/jfsCache/myjfs 2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT_A $MNT_B $MNT_C

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/round1.bin bs=1M count=2 2>/dev/null
    R1_HASH=$(md5_of $MNT_ORIG/round1.bin)
    sync

    # First round: create A and B, release both → sets forkProtectCleared=1
    ./juicefs fork create $META_URL $FORK_META_A --name fork-a
    ./juicefs fork create $META_URL $FORK_META_B --name fork-b
    ./juicefs fork release $META_URL --fork-name fork-b
    ./juicefs fork release $META_URL --fork-name fork-a
    # At this point forkProtectCleared=1 in source DB

    # Second round: write new data and create fork C
    dd if=/dev/urandom of=$MNT_ORIG/round2.bin bs=1M count=2 2>/dev/null
    R2_HASH=$(md5_of $MNT_ORIG/round2.bin)
    sync

    ./juicefs fork create $META_URL $FORK_META_C --name fork-c
    ./juicefs mount -d $FORK_META_C $MNT_C --no-usage-report
    sleep 1

    assert_file_exists $MNT_C/round1.bin
    assert_file_exists $MNT_C/round2.bin
    HASH_CHECK=$(md5_of $MNT_C/round2.bin)
    assert_eq "fork-c sees round2 at fork time" "$HASH_CHECK" "$R2_HASH"

    # Delete from source and GC — fork-c's data must be protected
    rm -f $MNT_ORIG/round1.bin $MNT_ORIG/round2.bin
    sync
    umount_jfs $MNT_ORIG "$META_URL"
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete

    # fork-c must still see both files (forkProtectCleared was re-armed)
    assert_file_exists $MNT_C/round1.bin
    assert_file_exists $MNT_C/round2.bin
    assert_eq "fork-c round1 intact after re-armed GC" "$(md5_of $MNT_C/round1.bin)" "$R1_HASH"
    assert_eq "fork-c round2 intact after re-armed GC" "$(md5_of $MNT_C/round2.bin)" "$R2_HASH"
    echo "fork-c protected after full release + recreate cycle (rearm fix) — OK"

    umount_jfs $MNT_C "$FORK_META_C" 2>/dev/null || true
    rm -f /tmp/jfs-recreate-a.db /tmp/jfs-recreate-b.db /tmp/jfs-recreate-c.db
}

# =============================================================================
# test_fork_trash_days_protection
#
# When TrashDays > 0, deleting a pre-fork file puts it in trash, not immediately
# removing slice references. GC's trash cleanup runs as part of --delete. The
# fork must still be able to read the file even after source trash is purged
# because slice protection is ID-based, not metadata-presence-based.
# =============================================================================
test_fork_trash_days_protection() {
    setup_two_mounts

    # TrashDays=1 so files go to trash before being purged
    ./juicefs format $META_URL myjfs --trash-days 1
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/will-trash.bin bs=1M count=2 2>/dev/null
    TRASH_HASH=$(md5_of $MNT_ORIG/will-trash.bin)
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    # Delete on source — with trash-days=1 this goes to trash, not immediate
    rm -f $MNT_ORIG/will-trash.bin
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    # GC with --delete runs trash cleanup (edge=-trashDays) and object scan
    # The trash hasn't expired (only just deleted), so chunks stay in pending
    # BUT fork protection by slice ID should prevent deletion either way
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete

    assert_file_exists $MNT_FORK/will-trash.bin
    assert_eq "fork sees trashed file" "$(md5_of $MNT_FORK/will-trash.bin)" "$TRASH_HASH"
    echo "fork protected from trashed pre-fork file GC — OK"

    umount_jfs $MNT_FORK "$FORK_META_URL" 2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_duplicate_name_rejected
#
# Creating two forks with the same --fork-name under the same source must fail
# on the second attempt (two leases with identical names are ambiguous for
# 'fork release' and 'fork list').
# =============================================================================
test_fork_duplicate_name_rejected() {
    FORK_META_A="sqlite3:///tmp/jfs-dupname-a.db"
    FORK_META_B="sqlite3:///tmp/jfs-dupname-b.db"
    MNT_A=/jfs-dupname-a

    umount_jfs $MNT_ORIG "$META_URL"    2>/dev/null || true
    umount_jfs $MNT_A    "$FORK_META_A" 2>/dev/null || true
    for db in "$META_URL" "$FORK_META_A" "$FORK_META_B"; do
        python3 .github/scripts/flush_meta.py "$db" 2>/dev/null || true
    done
    rm -f /tmp/jfs-dupname-a.db /tmp/jfs-dupname-b.db 2>/dev/null || true
    rm -rf /var/jfs/myjfs /var/jfsCache/myjfs 2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT_A

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    echo "data" > $MNT_ORIG/file.txt
    sync

    # First fork with name "my-snapshot" — succeeds
    ./juicefs fork create $META_URL $FORK_META_A --name my-snapshot
    echo "first fork succeeded — OK"

    # Second fork with same name to different destination — should fail
    if ./juicefs fork create $META_URL $FORK_META_B --name my-snapshot 2>&1; then
        echo "<FATAL> second fork with same name should have been rejected"
        exit 1
    fi
    echo "duplicate fork name rejected — OK"

    # fork list should show only one entry
    COUNT=$(./juicefs fork list $META_URL 2>&1 | grep -c "my-snapshot" || echo 0)
    assert_eq "only one fork with that name" "$COUNT" "1"

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    rm -f /tmp/jfs-dupname-a.db /tmp/jfs-dupname-b.db
}

# =============================================================================
# test_fork_release_twice_fails_cleanly
#
# Releasing a fork that doesn't exist (or releasing the same fork twice) must
# fail with a clear error, not silently succeed or corrupt state.
# =============================================================================
test_fork_release_twice_fails_cleanly() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    echo "data" > $MNT_ORIG/file.txt
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork

    # First release — succeeds
    ./juicefs fork release $META_URL --fork-name myjfs-fork
    echo "first release succeeded — OK"

    # Second release — must fail with clear error, not panic
    if ./juicefs fork release $META_URL --fork-name myjfs-fork 2>&1; then
        echo "<FATAL> releasing already-released fork should fail"
        exit 1
    fi
    echo "second release failed as expected — OK"

    # forkProtectCleared must be set (both releases completed)
    CLEARED=$(./juicefs config $META_URL 2>&1 | grep -c "forkProtectCleared" || true)
    # GC must not be blocked
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete
    echo "GC after double-release attempt runs cleanly — OK"

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_orphan_lease_in_bucket
#
# If the lease file exists in the bucket but the fork's metadata DB was wiped
# (simulating a partial failure or manual cleanup), GC must still refuse to
# delete the protected objects because the bucket lease is authoritative.
# =============================================================================
test_fork_orphan_lease_in_bucket() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/protected.bin bs=1M count=2 2>/dev/null
    HASH=$(md5_of $MNT_ORIG/protected.bin)
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork

    # Simulate: fork DB was wiped (orphan lease remains in bucket)
    python3 .github/scripts/flush_meta.py "$FORK_META_URL"
    rm -f /tmp/jfs-fork-test-fork.db

    # Delete from source and run GC
    rm -f $MNT_ORIG/protected.bin
    sync
    umount_jfs $MNT_ORIG "$META_URL"
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete

    # Bucket lease is still present → GC must not delete protected chunks
    # Verify source can still read the file (it's been deleted from source metadata)
    # Verify objects are still in the bucket (protected by orphan lease)
    OBJS=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")
    if [[ "$OBJS" -eq 0 ]]; then
        echo "<FATAL> expected objects still present due to orphan lease protection, got 0 objects"
        exit 1
    fi
    echo "orphan bucket lease protects chunks from GC — OK (scanned $OBJS objects)"

    # Clean up: release the orphan lease
    ./juicefs fork release $META_URL --fork-name myjfs-fork

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_list_empty_volume
#
# fork list on a volume that has never been forked must return empty cleanly
# without error, not segfault or return non-zero.
# =============================================================================
test_fork_list_empty_volume() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    OUTPUT=$(./juicefs fork list $META_URL 2>&1)
    if echo "$OUTPUT" | grep -qi "error\|panic\|fatal"; then
        echo "<FATAL> fork list on never-forked volume produced error: $OUTPUT"
        exit 1
    fi
    echo "fork list on never-forked volume returned cleanly — OK"

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_compact_source_fork_reads_correctly
#
# GC --compact on the source rewrites fragmented slices into new slice IDs.
# The OLD slice IDs (pre-fork, fork-protected) must NOT be deleted even though
# compaction created replacement slices. The fork must still read via the old
# slice IDs since its metadata still points to them.
# =============================================================================
test_fork_compact_source_fork_reads_correctly() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    # Create many small appends → multiple slice IDs for same logical chunk
    for i in $(seq 1 8); do
        dd if=/dev/urandom bs=128K count=1 >> $MNT_ORIG/fragmented.bin 2>/dev/null
    done
    BEFORE_HASH=$(md5_of $MNT_ORIG/fragmented.bin)
    sync

    # Take checkpoint (fork) before compaction
    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    FORK_HASH=$(md5_of $MNT_FORK/fragmented.bin)
    assert_eq "fork hash matches source before compaction" "$FORK_HASH" "$BEFORE_HASH"

    # Compact source: rewrites old slice IDs → new slice IDs
    # Old IDs are protected; GC must not delete them
    umount_jfs $MNT_ORIG "$META_URL"
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --compact --delete

    # Fork still reads via old slice IDs — must see same data
    assert_file_exists $MNT_FORK/fragmented.bin
    assert_eq "fork reads correctly via old slices post-compaction" \
        "$(md5_of $MNT_FORK/fragmented.bin)" "$BEFORE_HASH"
    echo "fork readable via old slice IDs after source compaction — OK"

    # Source remounts and reads new compacted slices
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    assert_file_exists $MNT_ORIG/fragmented.bin
    assert_eq "source reads compacted data" \
        "$(md5_of $MNT_ORIG/fragmented.bin)" "$BEFORE_HASH"
    echo "source reads compacted data correctly — OK"

    umount_jfs $MNT_ORIG "$META_URL" 2>/dev/null || true
    umount_jfs $MNT_FORK "$FORK_META_URL" 2>/dev/null || true
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_deferred_dump_load
#
# fork dump/load should:
#   1) write dump + manifest + lease
#   2) load into an empty destination DB
#   3) produce a mountable shared-storage fork
# =============================================================================
test_fork_deferred_dump_load() {
    local DUMP_PATH=/tmp/jfs-fork-deferred.json.gz
    local MANIFEST_PATH=${DUMP_PATH}.fork.json

    setup_two_mounts
    rm -f "$DUMP_PATH" "$MANIFEST_PATH"

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    mkdir -p $MNT_ORIG/deferred
    echo "deferred-flow" > $MNT_ORIG/deferred/file.txt
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    ./juicefs fork dump $META_URL --path "$DUMP_PATH" --name deferred-fork
    assert_file_exists "$DUMP_PATH"
    assert_file_exists "$MANIFEST_PATH"

    FORK_LIST=$(./juicefs fork list $META_URL)
    if [[ "$FORK_LIST" != *"deferred-fork"* ]]; then
        echo "<FATAL> fork list does not contain deferred-fork after fork dump"
        exit 1
    fi

    ./juicefs fork load $FORK_META_URL --path "$DUMP_PATH"
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1

    assert_file_exists $MNT_FORK/deferred/file.txt
    assert_eq "deferred fork content" "$(cat $MNT_FORK/deferred/file.txt)" "deferred-flow"

    FORK_UUID=$(./juicefs status $FORK_META_URL 2>&1 | grep -oP '"UUID":\s*"[^"]+"' | grep -oP '[0-9a-f-]{36}' | head -1)
    if [[ -z "$FORK_UUID" ]]; then
        echo "<FATAL> could not get fork UUID from deferred fork volume"
        exit 1
    fi

    umount_jfs $MNT_FORK "$FORK_META_URL"
    ./juicefs destroy --yes "$FORK_META_URL" "$FORK_UUID"
    ./juicefs fork release $META_URL --fork-name deferred-fork

    rm -f "$DUMP_PATH" "$MANIFEST_PATH"
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_dump_lease_protects_before_load
#
# Lease written by fork dump must protect pre-fork objects from source GC
# even before fork load is executed.
# =============================================================================
test_fork_dump_lease_protects_before_load() {
    local DUMP_PATH=/tmp/jfs-fork-preload.json
    local MANIFEST_PATH=${DUMP_PATH}.fork.json

    setup_two_mounts
    rm -f "$DUMP_PATH" "$MANIFEST_PATH"

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    echo "protected-before-load" > $MNT_ORIG/protected-before-load.txt
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    ./juicefs fork dump $META_URL --path "$DUMP_PATH" --name pre-load-protect

    # Delete from source metadata and run GC while lease is active (before load).
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    rm -f $MNT_ORIG/protected-before-load.txt
    sync
    umount_jfs $MNT_ORIG "$META_URL"
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete
    GC_SCAN_WITH_LEASE=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")
    if [[ "$GC_SCAN_WITH_LEASE" -eq 0 ]]; then
        echo "<FATAL> expected protected objects to remain before fork load (lease active)"
        exit 1
    fi

    # Source metadata path should be gone after delete+GC.
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    assert_file_missing $MNT_ORIG/protected-before-load.txt
    umount_jfs $MNT_ORIG "$META_URL"

    # Now load fork and verify protected content is still readable.
    ./juicefs fork load $FORK_META_URL --path "$DUMP_PATH"
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1
    assert_file_exists $MNT_FORK/protected-before-load.txt
    assert_eq "pre-load lease protection" \
        "$(cat $MNT_FORK/protected-before-load.txt)" "protected-before-load"

    FORK_UUID=$(./juicefs status $FORK_META_URL 2>&1 | grep -oP '"UUID":\s*"[^"]+"' | grep -oP '[0-9a-f-]{36}' | head -1)
    if [[ -z "$FORK_UUID" ]]; then
        echo "<FATAL> could not get fork UUID from loaded deferred fork"
        exit 1
    fi

    umount_jfs $MNT_FORK "$FORK_META_URL"
    ./juicefs destroy --yes "$FORK_META_URL" "$FORK_UUID"
    ./juicefs fork release $META_URL --fork-name pre-load-protect

    rm -f "$DUMP_PATH" "$MANIFEST_PATH"
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_deferred_load_corrupt_dump
#
# Corrupted dump file must cause fork load to fail.
# =============================================================================
test_fork_deferred_load_corrupt_dump() {
    local DUMP_PATH=/tmp/jfs-fork-corrupt.json
    local MANIFEST_PATH=${DUMP_PATH}.fork.json

    setup_two_mounts
    rm -f "$DUMP_PATH" "$MANIFEST_PATH"

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    echo "corrupt-me" > $MNT_ORIG/corrupt.txt
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    ./juicefs fork dump $META_URL --path "$DUMP_PATH" --name deferred-corrupt
    # Overwrite with malformed JSON so load must fail deterministically.
    printf '{invalid-json\n' > "$DUMP_PATH"

    if LOAD_ERR=$(./juicefs fork load $FORK_META_URL --path "$DUMP_PATH" 2>&1); then
        echo "<FATAL> fork load should fail on corrupted dump data"
        exit 1
    fi
    if [[ "$LOAD_ERR" != *"load json dump"* ]]; then
        echo "<FATAL> unexpected fork load failure path for corrupt dump"
        echo "$LOAD_ERR"
        exit 1
    fi
    echo "fork load failed on corrupted dump as expected — OK"

    ./juicefs fork release $META_URL --fork-name deferred-corrupt
    python3 .github/scripts/flush_meta.py "$FORK_META_URL" 2>/dev/null || true
    rm -f "$DUMP_PATH" "$MANIFEST_PATH"
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_deferred_dump_rollback_on_dump_error
#
# If fork dump fails after lease reservation (e.g. dump path is invalid),
# rollback must remove the temporary lease and GC should not stay blocked.
# =============================================================================
test_fork_deferred_dump_rollback_on_dump_error() {
    local BAD_DIR=/tmp/jfs-fork-dump-rollback-dir
    local BAD_DUMP_PATH=${BAD_DIR}/meta.json
    local FAILED_NAME=deferred-rollback-failed
    local SEED_NAME=deferred-rollback-seed
    local GC_OUT=""

    setup_two_mounts
    rm -rf "$BAD_DIR"

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    echo "seed" > $MNT_ORIG/seed.txt
    sync

    # First create+release a seed fork so the next fork-dump path re-arms
    # protection counters before failing.
    ./juicefs fork create $META_URL $FORK_META_URL --name "$SEED_NAME"
    SEED_UUID=$(./juicefs status $FORK_META_URL 2>&1 | grep -oP '"UUID":\s*"[^"]+"' | grep -oP '[0-9a-f-]{36}' | head -1)
    if [[ -z "$SEED_UUID" ]]; then
        echo "<FATAL> could not get seed fork UUID"
        exit 1
    fi
    ./juicefs destroy --yes "$FORK_META_URL" "$SEED_UUID"
    ./juicefs fork release $META_URL --fork-name "$SEED_NAME"

    umount_jfs $MNT_ORIG "$META_URL"

    # This must fail after lease reservation because target directory is missing.
    if DUMP_ERR=$(./juicefs fork dump $META_URL --path "$BAD_DUMP_PATH" --name "$FAILED_NAME" 2>&1); then
        echo "<FATAL> fork dump should fail on invalid dump path"
        exit 1
    fi
    if [[ "$DUMP_ERR" != *"dump metadata to"* ]]; then
        echo "<FATAL> unexpected fork dump failure path"
        echo "$DUMP_ERR"
        exit 1
    fi

    # Rollback should have removed the temporary lease.
    FORK_LIST=$(./juicefs fork list $META_URL)
    if [[ "$FORK_LIST" == *"$FAILED_NAME"* ]]; then
        echo "<FATAL> rollback failed: temporary lease still listed after failed fork dump"
        exit 1
    fi
    if ./juicefs fork release $META_URL --fork-name "$FAILED_NAME" 2>&1; then
        echo "<FATAL> rollback failed: failed dump lease is still releasable"
        exit 1
    fi

    GC_OUT=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1)
    if [[ "$GC_OUT" == *"Fork protection active: protecting objects"* ]]; then
        echo "<FATAL> GC still reports active fork protection after failed dump rollback with no leases"
        echo "$GC_OUT"
        exit 1
    fi
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete

    rm -rf "$BAD_DIR"
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_deferred_load_sqlite_wal_checkpoint
#
# After fork load to sqlite destination, DB should be self-contained without
# requiring -wal/-shm sidecars.
# =============================================================================
test_fork_deferred_load_sqlite_wal_checkpoint() {
    [[ "$META" != "sqlite3" ]] && echo "Skip: WAL checkpoint test only for sqlite3" && return 0

    local DUMP_PATH=/tmp/jfs-fork-deferred-wal.json
    local MANIFEST_PATH=${DUMP_PATH}.fork.json
    local COPY_DB=/tmp/jfs-fork-deferred-wal-copy.db

    setup_two_mounts
    rm -f "$DUMP_PATH" "$MANIFEST_PATH" "$COPY_DB" "${COPY_DB}-wal" "${COPY_DB}-shm"

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    mkdir -p $MNT_ORIG/waltest
    for i in {1..10}; do echo "deferred-wal-$i" > $MNT_ORIG/waltest/file$i.txt; done
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    ./juicefs fork dump $META_URL --path "$DUMP_PATH" --name deferred-wal
    ./juicefs fork load $FORK_META_URL --path "$DUMP_PATH"

    FORK_DB_PATH="${FORK_META_URL#sqlite3://}"
    if [[ -f "${FORK_DB_PATH}-wal" ]]; then
        echo "<FATAL> ${FORK_DB_PATH}-wal still exists after fork load"
        exit 1
    fi
    if [[ -f "${FORK_DB_PATH}-shm" ]]; then
        echo "<FATAL> ${FORK_DB_PATH}-shm still exists after fork load"
        exit 1
    fi
    echo "No sqlite -wal/-shm sidecars after fork load — OK"

    cp "$FORK_DB_PATH" "$COPY_DB"
    ./juicefs mount -d "sqlite3://$COPY_DB" $MNT_FORK --no-usage-report
    sleep 1
    COUNT=$(ls $MNT_FORK/waltest/ 2>/dev/null | wc -l)
    if [[ "$COUNT" -ne 10 ]]; then
        echo "<FATAL> expected 10 files in deferred fork sqlite copy, got $COUNT"
        exit 1
    fi
    umount_jfs $MNT_FORK "sqlite3://$COPY_DB"

    FORK_UUID=$(./juicefs status $FORK_META_URL 2>&1 | grep -oP '"UUID":\s*"[^"]+"' | grep -oP '[0-9a-f-]{36}' | head -1)
    ./juicefs destroy --yes "$FORK_META_URL" "$FORK_UUID"
    ./juicefs fork release $META_URL --fork-name deferred-wal

    rm -f "$DUMP_PATH" "$MANIFEST_PATH" "$COPY_DB" "${COPY_DB}-wal" "${COPY_DB}-shm"
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_dump_load_independent_volume
#
# juicefs dump + juicefs load produces a new independent volume with fresh
# counters and a new UUID. Forking that loaded volume must work correctly —
# the new fork's forkBaseChunk is based on the loaded volume's nextChunk, which
# starts fresh, so it won't accidentally overlap with the original bucket.
# The loaded volume is NOT a shared-storage fork; it's a standalone copy.
# Destroying the loaded volume's fork should work normally.
# =============================================================================
test_fork_dump_load_independent_volume() {
    LOADED_META="sqlite3:///tmp/jfs-dumpload-loaded.db"
    LOADED_FORK_META="sqlite3:///tmp/jfs-dumpload-fork.db"
    MNT_LOADED=/jfs-dumpload-loaded
    MNT_LOADED_FORK=/jfs-dumpload-fork

    umount_jfs $MNT_ORIG        "$META_URL"        2>/dev/null || true
    umount_jfs $MNT_LOADED      "$LOADED_META"     2>/dev/null || true
    umount_jfs $MNT_LOADED_FORK "$LOADED_FORK_META" 2>/dev/null || true
    for db in "$META_URL" "$LOADED_META" "$LOADED_FORK_META"; do
        python3 .github/scripts/flush_meta.py "$db" 2>/dev/null || true
    done
    rm -f /tmp/jfs-dumpload-loaded.db /tmp/jfs-dumpload-fork.db 2>/dev/null || true
    rm -rf /var/jfs/myjfs /var/jfsCache/myjfs 2>/dev/null || true
    mkdir -p $MNT_ORIG $MNT_LOADED $MNT_LOADED_FORK

    # Source with some data
    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1
    echo "original data" > $MNT_ORIG/orig.txt
    dd if=/dev/urandom of=$MNT_ORIG/bigfile.bin bs=1M count=2 2>/dev/null
    ORIG_HASH=$(md5_of $MNT_ORIG/bigfile.bin)
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    # Dump source metadata, load into a new volume (load requires empty DB — no format first)
    ./juicefs dump $META_URL /tmp/jfs-dumpload.json.gz
    ./juicefs load $LOADED_META /tmp/jfs-dumpload.json.gz
    rm -f /tmp/jfs-dumpload.json.gz

    # Fork the loaded volume — should work, loaded vol is standalone
    ./juicefs fork create $LOADED_META $LOADED_FORK_META --name loaded-fork

    FORK_UUID=$(./juicefs status $LOADED_FORK_META 2>&1 | grep -oP '"UUID":\s*"[^"]+"' | grep -oP '[0-9a-f-]{36}' | head -1)
    if [[ -z "$FORK_UUID" ]]; then
        echo "<FATAL> could not get fork UUID from loaded fork"
        exit 1
    fi

    # Destroy the loaded fork (it's standalone, destroy should work normally)
    # Since loaded-fork is not a real shared-storage fork (no forkSharedStorage
    # counter from the dump), destroy should proceed as normal
    # But since we just created the fork, the loaded volume has an active lease
    # — destroy source (loaded-vol) should be rejected
    LOADED_UUID=$(./juicefs status $LOADED_META 2>&1 | grep -oP '"UUID":\s*"[^"]+"' | grep -oP '[0-9a-f-]{36}' | head -1)
    if ./juicefs destroy --yes "$LOADED_META" "$LOADED_UUID" 2>&1; then
        echo "<FATAL> destroying loaded-vol with active fork lease should be rejected"
        exit 1
    fi
    echo "destroying loaded volume with active fork lease rejected — OK"

    # Release the fork lease, then destroy should work
    ./juicefs fork release $LOADED_META --fork-name loaded-fork
    if ! ./juicefs destroy --yes "$LOADED_META" "$LOADED_UUID" 2>&1; then
        echo "<FATAL> destroying loaded-vol after fork release should succeed"
        exit 1
    fi
    echo "loaded volume destroyed after fork release — OK"

    umount_jfs $MNT_LOADED      "$LOADED_META"      2>/dev/null || true
    umount_jfs $MNT_LOADED_FORK "$LOADED_FORK_META" 2>/dev/null || true
    rm -f /tmp/jfs-dumpload-loaded.db /tmp/jfs-dumpload-fork.db
}

# =============================================================================
# test_fork_gc_cleans_up_after_all_forks_released
#
# After releasing ALL forks, GC must fully reclaim all unreferenced objects.
# Specifically: no active leases and protection counters marked cleared
# (forkProtectCleared advanced beyond forkProtectRearm) → GC runs freely.
# Verifies that the "cleared" path actually unblocks reclamation end-to-end.
# =============================================================================
test_fork_gc_cleans_up_after_all_forks_released() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/reclaimable.bin bs=1M count=4 2>/dev/null
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork

    # Delete from source, GC blocked by lease
    rm -f $MNT_ORIG/reclaimable.bin
    sync
    umount_jfs $MNT_ORIG "$META_URL"
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete

    OBJS_WITH_LEASE=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")
    if [[ "$OBJS_WITH_LEASE" -eq 0 ]]; then
        echo "<FATAL> expected objects to survive GC while lease active"
        exit 1
    fi
    echo "objects protected while lease active (scanned=$OBJS_WITH_LEASE) — OK"

    # Release lease
    ./juicefs fork release $META_URL --fork-name myjfs-fork

    # GC now must reclaim
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete
    OBJS_AFTER=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")

    if [[ "$OBJS_AFTER" -ge "$OBJS_WITH_LEASE" ]]; then
        echo "<FATAL> GC did not reclaim after lease release (before=$OBJS_WITH_LEASE after=$OBJS_AFTER)"
        exit 1
    fi
    echo "GC fully reclaimed after all leases released (before=$OBJS_WITH_LEASE after=$OBJS_AFTER) — OK"

    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_sqlite_wal_checkpoint
#
# Verifies that after `juicefs fork`, the destination SQLite .db file is
# fully self-contained (WAL checkpointed). Copying just the .db file —
# without the -wal and -shm sidecars — must produce a mountable volume
# with all pre-fork data intact.
#
# Background: SQLite WAL mode writes data to a -wal sidecar first. Without
# an explicit db.Close(), only the .db header is flushed and the main file
# appears empty ("database is not formatted"). The fix calls dstMeta.Shutdown()
# at the end of forkCreate to force the checkpoint.
# =============================================================================
test_fork_sqlite_wal_checkpoint() {
    [[ "$META" != "sqlite3" ]] && echo "Skip: WAL test only relevant for sqlite3" && return 0

    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    # Write some data so there is real metadata in the fork DB
    mkdir -p $MNT_ORIG/waldir
    for i in {1..10}; do echo "wal-content-$i" > $MNT_ORIG/waldir/file$i.txt; done
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    # Create fork
    ./juicefs fork create $META_URL $FORK_META_URL --name wal-ckpt-test

    FORK_DB_PATH="${FORK_META_URL#sqlite3://}"

    # After fork, -wal and -shm must not exist (checkpoint was done)
    if [[ -f "${FORK_DB_PATH}-wal" ]]; then
        echo "<FATAL> ${FORK_DB_PATH}-wal still exists after fork — WAL not checkpointed"
        exit 1
    fi
    echo "No -wal sidecar after fork — OK"

    # Copy only the .db file to simulate a user transferring just fork.db
    local COPY_DB=/tmp/jfs-fork-wal-copy.db
    rm -f "$COPY_DB" "${COPY_DB}-wal" "${COPY_DB}-shm"
    cp "$FORK_DB_PATH" "$COPY_DB"

    # Mount the single-file copy — must work and show all pre-fork files
    mkdir -p $MNT_FORK
    ./juicefs mount -d "sqlite3://$COPY_DB" $MNT_FORK --no-usage-report
    sleep 1

    local count
    count=$(ls $MNT_FORK/waldir/ 2>/dev/null | wc -l)
    if [[ "$count" -ne 10 ]]; then
        echo "<FATAL> expected 10 files in fork copy, got $count — WAL data not in .db"
        exit 1
    fi
    echo "All 10 files visible from single-file fork copy — OK"

    local content
    content=$(cat $MNT_FORK/waldir/file1.txt)
    if [[ "$content" != "wal-content-1" ]]; then
        echo "<FATAL> unexpected content in fork copy: $content"
        exit 1
    fi
    echo "File content correct — OK"

    umount_jfs $MNT_FORK "sqlite3://$COPY_DB"
    rm -f "$COPY_DB"
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_release_from_any_peer
#
# Expectation: fork release can be performed from ANY volume that shares the
# same bucket — not just the source.  This is the "no master/slave" invariant:
# every DB sharing a bucket has equal rights over fork leases.
#
# Scenario:
#   1. Source → Fork A
#   2. Release A's lease by passing Fork A's own meta URI (not source)
#   3. Must succeed — lease deleted from shared bucket, protection cleared.
# =============================================================================
test_fork_release_from_any_peer() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/data.bin bs=1M count=2 2>/dev/null
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork

    # Release from the FORK's meta URL — not the source
    ./juicefs fork release $FORK_META_URL --fork-name myjfs-fork

    # Verify lease is gone — list from source should show empty
    LEASES=$(./juicefs fork list $META_URL 2>&1)
    if echo "$LEASES" | grep -q "myjfs-fork"; then
        echo "<FATAL> lease still visible after release from fork peer"
        exit 1
    fi
    echo "release from fork peer succeeded — OK"

    # Also verify listing from fork peer shows the same result
    LEASES_FROM_FORK=$(./juicefs fork list $FORK_META_URL 2>&1)
    if echo "$LEASES_FROM_FORK" | grep -q "myjfs-fork"; then
        echo "<FATAL> lease still visible when listing from fork peer"
        exit 1
    fi
    echo "fork list from fork peer consistent — OK"

    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_release_after_source_destroyed
#
# Expectation: if the source volume's metadata DB is destroyed (gone), a
# surviving fork can still release its own lease from the shared bucket.
#
# This proves the "no master" architecture: leases live in the bucket, and
# any volume with access to that bucket can manage them.
#
# Scenario:
#   1. Source → Fork A
#   2. Destroy source DB entirely
#   3. Release A's lease using Fork A's meta URI
#   4. Must succeed — bucket lease deleted, fork list (from fork) shows empty
# =============================================================================
test_fork_release_after_source_destroyed() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/data.bin bs=1M count=2 2>/dev/null
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork

    # Nuke the source DB — simulating source volume deletion
    SRC_DB="${META_URL#sqlite3://}"
    rm -f "$SRC_DB" "${SRC_DB}-wal" "${SRC_DB}-shm"

    # Release from the fork — source is gone
    ./juicefs fork release $FORK_META_URL --fork-name myjfs-fork

    # Verify lease is gone — list from fork should show empty
    LEASES=$(./juicefs fork list $FORK_META_URL 2>&1)
    if echo "$LEASES" | grep -q "myjfs-fork"; then
        echo "<FATAL> lease still visible after release (source destroyed)"
        exit 1
    fi
    echo "release after source destroyed succeeded — OK"

    # Fork should still be mountable
    ./juicefs mount -d $FORK_META_URL $MNT_FORK --no-usage-report
    sleep 1
    assert_file_exists $MNT_FORK/data.bin
    echo "fork still mountable after source destroyed — OK"

    umount_jfs $MNT_FORK "$FORK_META_URL"
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_of_fork_release_from_grandchild
#
# Expectation: in a chain Source → Fork A → Fork B, releasing Fork A's
# lease can be done from Fork B's meta URI.  All three share one bucket;
# any peer can manage any lease.
# =============================================================================
test_fork_of_fork_release_from_grandchild() {
    setup_two_mounts

    local FORK_B_META="sqlite3:///tmp/jfs-fork-test-fork-b.db"
    local MNT_FORK_B=/jfs-fork-b
    mkdir -p $MNT_FORK_B

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    echo "grandparent" > $MNT_ORIG/root.txt
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    # Source → Fork A
    ./juicefs fork create $META_URL $FORK_META_URL --name fork-a

    # Fork A → Fork B
    ./juicefs fork create $FORK_META_URL $FORK_B_META --name fork-b

    # Release Fork A's lease from Fork B's URI
    ./juicefs fork release $FORK_B_META --fork-name fork-a

    # Verify fork-a lease is gone, fork-b still present
    LEASES=$(./juicefs fork list $META_URL 2>&1)
    if echo "$LEASES" | grep -q "fork-a"; then
        echo "<FATAL> fork-a lease still visible after release from grandchild"
        exit 1
    fi
    if ! echo "$LEASES" | grep -q "fork-b"; then
        echo "<FATAL> fork-b lease should still be active"
        exit 1
    fi
    echo "release fork-a from grandchild (fork-b) succeeded — OK"

    # Cleanup
    ./juicefs fork release $META_URL --fork-name fork-b
    umount_jfs $MNT_FORK_B "$FORK_B_META" 2>/dev/null || true
    rm -f /tmp/jfs-fork-test-fork-b.db /tmp/jfs-fork-test-fork-b.db-wal /tmp/jfs-fork-test-fork-b.db-shm
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_list_from_fork_peer
#
# Expectation: `fork list` called with a fork's own meta URI returns the same
# lease information as when called with the source's meta URI.  This verifies
# the "no master/slave" invariant for the list subcommand while both source
# and fork are alive and leases are active.
# =============================================================================
test_fork_list_from_fork_peer() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    echo "data" > $MNT_ORIG/file.txt
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork

    # List from source
    LEASES_SRC=$(./juicefs fork list $META_URL 2>&1)
    if ! echo "$LEASES_SRC" | grep -q "myjfs-fork"; then
        echo "<FATAL> lease not visible when listing from source"
        exit 1
    fi

    # List from fork — must show the same lease
    LEASES_FORK=$(./juicefs fork list $FORK_META_URL 2>&1)
    if ! echo "$LEASES_FORK" | grep -q "myjfs-fork"; then
        echo "<FATAL> lease not visible when listing from fork peer"
        exit 1
    fi
    echo "fork list from fork peer shows active leases — OK"

    # Cleanup
    ./juicefs fork release $META_URL --fork-name myjfs-fork
    flush_fork_sqlite_dbs
}

# =============================================================================
# test_fork_gc_reclaims_after_cross_peer_release
#
# Expectation: when a fork lease is released from the fork's own meta URI
# (not the source), GC on the SOURCE must still reclaim pre-fork objects.
#
# This is the critical GC + "no master/slave" scenario.  The source's DB
# counters are NOT updated by the cross-peer release, so GC must consult the
# bucket (authoritative) and see zero leases → protection off → reclaim.
# =============================================================================
test_fork_gc_reclaims_after_cross_peer_release() {
    setup_two_mounts

    ./juicefs format $META_URL myjfs --trash-days 0
    ./juicefs mount -d $META_URL $MNT_ORIG --no-usage-report
    sleep 1

    dd if=/dev/urandom of=$MNT_ORIG/reclaimable.bin bs=1M count=4 2>/dev/null
    sync

    ./juicefs fork create $META_URL $FORK_META_URL --name myjfs-fork

    # Delete from source so chunks become reclaimable once protection lifts
    rm -f $MNT_ORIG/reclaimable.bin
    sync
    umount_jfs $MNT_ORIG "$META_URL"

    # GC with lease active — objects must survive
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete
    OBJS_BEFORE=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")
    if [[ "$OBJS_BEFORE" -eq 0 ]]; then
        echo "<FATAL> expected objects to survive GC while lease active"
        exit 1
    fi
    echo "objects protected while lease active (scanned=$OBJS_BEFORE) — OK"

    # Release from the FORK (not source) — source DB counters NOT updated
    ./juicefs fork release $FORK_META_URL --fork-name myjfs-fork

    # GC on SOURCE must still reclaim — bucket has no leases
    JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL --delete
    OBJS_AFTER=$(JFS_GC_SKIPPEDTIME=0 ./juicefs gc $META_URL 2>&1 | grep -oP 'scanned \d+' | grep -oP '\d+' || echo "0")

    if [[ "$OBJS_AFTER" -ge "$OBJS_BEFORE" ]]; then
        echo "<FATAL> GC on source did not reclaim after cross-peer release (before=$OBJS_BEFORE after=$OBJS_AFTER)"
        exit 1
    fi
    echo "GC reclaimed after cross-peer release (before=$OBJS_BEFORE after=$OBJS_AFTER) — OK"

    flush_fork_sqlite_dbs
}

source .github/scripts/common/run_test.sh && run_test $@
