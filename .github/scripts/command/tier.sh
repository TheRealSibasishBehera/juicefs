#!/bin/bash -e

[[ -z "$META" ]] && META=redis
source .github/scripts/start_meta_engine.sh
start_meta_engine $META
META_URL=$(get_meta_url $META)
source .github/scripts/common/common.sh

AWS_BUCKET=${AWS_BUCKET:-tiertest-${META}}
AWS_BUCKET=$(printf '%s' "$AWS_BUCKET" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9.-' '-')
AWS_BUCKET=${AWS_BUCKET#-}
AWS_BUCKET=${AWS_BUCKET%-}
AWS_REGION=${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}
AWS_ACCESS_KEY_VALUE=${AWS_ACCESS_KEY_ID:-${AWS_ACEESS_KEY:-}}
AWS_SECRET_KEY_VALUE=${AWS_SECRET_ACCESS_KEY:-}
AWS_SESSION_TOKEN_VALUE=${AWS_SESSION_TOKEN:-${AWS_ACCESS_TOKEN:-}}
ASSERT_RETRY_TIMES=${ASSERT_RETRY_TIMES:-30}
ASSERT_RETRY_INTERVAL=${ASSERT_RETRY_INTERVAL:-1}
if [[ "$AWS_REGION" == cn-* ]]; then
    DEFAULT_AWS_ENDPOINT_URL="https://s3.${AWS_REGION}.amazonaws.com.cn"
else
    DEFAULT_AWS_ENDPOINT_URL="https://s3.${AWS_REGION}.amazonaws.com"
fi
AWS_ENDPOINT_URL=${AWS_ENDPOINT_URL:-${AWS_S3_ENDPOINT_URL:-$DEFAULT_AWS_ENDPOINT_URL}}
AWS_BUCKET_URL=${AWS_BUCKET_URL:-${AWS_ENDPOINT_URL}/${AWS_BUCKET}}

ensure_aws_cli()
{
    if command -v aws >/dev/null 2>&1; then
        return 0
    fi
    if [[ "$PLATFORM" == "linux" ]]; then
        sudo .github/scripts/apt_install.sh awscli
    elif [[ "$PLATFORM" == "mac" ]]; then
        brew install awscli
    else
        echo "<FATAL>: unsupported platform for aws cli installation: $PLATFORM"
        exit 1
    fi
}

setup_aws_credentials()
{
    [[ -z "$AWS_ACCESS_KEY_VALUE" ]] && echo "<FATAL>: AWS access key is empty, set AWS_ACCESS_KEY_ID (or AWS_ACEESS_KEY)" && exit 1
    [[ -z "$AWS_SECRET_KEY_VALUE" ]] && echo "<FATAL>: AWS secret key is empty, set AWS_SECRET_ACCESS_KEY" && exit 1

    aws configure set aws_access_key_id "$AWS_ACCESS_KEY_VALUE"
    aws configure set aws_secret_access_key "$AWS_SECRET_KEY_VALUE"
    aws configure set default.region "$AWS_REGION"
    aws configure set default.output json
    if [[ -n "$AWS_SESSION_TOKEN_VALUE" ]]; then
        aws configure set aws_session_token "$AWS_SESSION_TOKEN_VALUE"
    fi

    aws sts get-caller-identity >/tmp/aws.identity.json
    local ak
    ak=$(aws configure get aws_access_key_id || true)
    echo "aws configured: region=$(aws configure get default.region || true), endpoint=$AWS_ENDPOINT_URL, bucket=$AWS_BUCKET"
    [[ -n "$ak" ]] && echo "aws configured access key prefix: ${ak:0:4}****"
    cat /tmp/aws.identity.json || true
}

recreate_aws_bucket_once()
{
    echo "recreate aws bucket: $AWS_BUCKET in region $AWS_REGION"
    aws s3 rb "s3://$AWS_BUCKET" --force --endpoint-url "$AWS_ENDPOINT_URL" >/dev/null 2>&1 || true

    if [[ "$AWS_REGION" == "us-east-1" ]]; then
        aws s3api create-bucket --bucket "$AWS_BUCKET" --endpoint-url "$AWS_ENDPOINT_URL" >/dev/null
    else
        aws s3api create-bucket \
            --bucket "$AWS_BUCKET" \
            --region "$AWS_REGION" \
            --endpoint-url "$AWS_ENDPOINT_URL" \
            --create-bucket-configuration LocationConstraint="$AWS_REGION" >/dev/null
    fi

    aws s3api wait bucket-exists --bucket "$AWS_BUCKET" --endpoint-url "$AWS_ENDPOINT_URL"
    aws s3api head-bucket --bucket "$AWS_BUCKET" --endpoint-url "$AWS_ENDPOINT_URL" >/tmp/aws.head_bucket.log 2>/tmp/aws.head_bucket.err || {
        cat /tmp/aws.head_bucket.err || true
        echo "<FATAL>: head-bucket failed for $AWS_BUCKET"
        exit 1
    }
    echo "aws bucket is ready: $AWS_BUCKET"
}

init_aws_bucket()
{
    ensure_aws_cli
    setup_aws_credentials
    recreate_aws_bucket_once
}

setup_tier_volume()
{
    prepare_test
    recreate_aws_bucket_once
    local format_cmd=(
        ./juicefs format "$META_URL" myjfs
        --storage s3
        --bucket "$AWS_BUCKET_URL"
        --access-key "$AWS_ACCESS_KEY_VALUE"
        --secret-key "$AWS_SECRET_KEY_VALUE"
        --trash-days 0
    )
    [[ -n "$AWS_SESSION_TOKEN_VALUE" ]] && format_cmd+=(--session-token "$AWS_SESSION_TOKEN_VALUE")

    "${format_cmd[@]}"
    ./juicefs mount -d "$META_URL" /jfs --heartbeat 2s

    # configure tier 1~3 before using juicefs tier commands
    ./juicefs config "$META_URL" --tier-id 1 --tier-sc STANDARD_IA -y
    ./juicefs config "$META_URL" --tier-id 2 --tier-sc INTELLIGENT_TIERING -y
    ./juicefs config "$META_URL" --tier-id 3 --tier-sc GLACIER_IR -y
}

get_tier_token()
{
    local path=$1
    local token
    token=$(./juicefs info "$path" | awk '/tier:/ {print $2; exit}')
    [[ -z "$token" ]] && return 1
    echo "$token"
}

assert_tier_id()
{
    local path=$1
    local expected=$2
    local token actual attempt
    for attempt in $(seq 1 "$ASSERT_RETRY_TIMES"); do
        token=$(get_tier_token "$path" 2>/dev/null || true)
        actual=${token%%->*}
        if [[ -n "$token" && "$actual" == "$expected" ]]; then
            return 0
        fi
        echo "wait tier id for $path, expect=$expected actual=${actual:-<empty>} attempt=$attempt/$ASSERT_RETRY_TIMES"
        sleep "$ASSERT_RETRY_INTERVAL"
    done
    echo "<FATAL>: tier id mismatch for $path, expect=$expected actual=${actual:-<empty>}"
    exit 1
}

assert_tier_sc()
{
    local path=$1
    local expected=$2
    local token actual attempt
    for attempt in $(seq 1 "$ASSERT_RETRY_TIMES"); do
        token=$(get_tier_token "$path" 2>/dev/null || true)
        actual=${token#*->}
        if [[ -n "$token" && "$actual" == "$expected" ]]; then
            return 0
        fi
        echo "wait tier storage class for $path, expect=$expected actual=${actual:-<empty>} attempt=$attempt/$ASSERT_RETRY_TIMES"
        sleep "$ASSERT_RETRY_INTERVAL"
    done
    echo "<FATAL>: tier storage class mismatch for $path, expect=$expected actual=${actual:-<empty>}"
    exit 1
}

assert_config_tier_sc_fail()
{
    local id=$1
    local sc=$2
    if ./juicefs config "$META_URL" --tier-id "$id" --tier-sc "$sc" -y; then
        echo "<FATAL>: expect config failure but succeeded, id=$id storage-class=$sc"
        exit 1
    fi
}

tier_set_no_err()
{
    local tmpout=/tmp/tier_set_last.log
    local status
    ./juicefs tier set "$@" 2>&1 | tee "$tmpout"
    status=${PIPESTATUS[0]}
    if grep -qF '<ERROR>' "$tmpout"; then
        echo "<FATAL>: juicefs tier set produced unexpected ERROR logs:"
        grep -F '<ERROR>' "$tmpout"
#        exit 1
    fi
    return "$status"
}

get_first_object_key()
{
    local path=$1
    local obj
    obj=$(./juicefs info "$path" | grep -o 'myjfs/chunks/[^[:space:]|]*' | head -n 1)
    [[ -z "$obj" ]] && return 1
    echo "$obj"
}

get_object_storage_class()
{
    local key=$1
    local storage_class
    storage_class=$(aws s3api head-object \
        --bucket "$AWS_BUCKET" \
        --key "$key" \
        --endpoint-url "$AWS_ENDPOINT_URL" \
        --query 'StorageClass' \
        --output text 2>/tmp/tier_head_object.err) || return 1
    [[ "$storage_class" == "None" || "$storage_class" == "null" || "$storage_class" == "" ]] && storage_class="STANDARD"
    echo "$storage_class"
}

assert_object_storage_class_by_path()
{
    local path=$1
    local expected=$2
    local key actual attempt
    for attempt in $(seq 1 "$ASSERT_RETRY_TIMES"); do
        key=$(get_first_object_key "$path" 2>/dev/null || true)
        if [[ -z "$key" && "$attempt" -eq 1 ]]; then
            echo "debug: no chunk object key parsed from juicefs info for $path"
            ./juicefs info "$path" | tee /tmp/tier_info_missing_key.log || true
        fi
        if [[ -n "$key" ]]; then
            actual=$(get_object_storage_class "$key" 2>/dev/null || true)
        else
            actual=""
        fi
        if [[ -n "$key" && "$actual" == "$expected" ]]; then
            return 0
        fi
        echo "wait object storage class for $path, key=${key:-<empty>} expect=$expected actual=${actual:-<empty>} attempt=$attempt/$ASSERT_RETRY_TIMES"
        sleep "$ASSERT_RETRY_INTERVAL"
    done
    [[ -f /tmp/tier_head_object.err ]] && cat /tmp/tier_head_object.err || true
    echo "<FATAL>: object storage class mismatch for $path key=${key:-<empty>} expect=$expected actual=${actual:-<empty>}"
    exit 1
}

test_tier_list_and_file_set_conversion()
{
    setup_tier_volume

    ./juicefs tier list "$META_URL" | tee /tmp/tier.list.log
    mkdir -p /jfs/file_case
    dd if=/dev/urandom of=/jfs/file_case/f1 bs=1M count=8 status=none

    tier_set_no_err "$META_URL" --id 1 /file_case/f1
    sleep 5
    assert_tier_id /jfs/file_case/f1 1
    ./juicefs info /jfs/file_case/f1
    assert_tier_sc /jfs/file_case/f1 STANDARD_IA
    assert_object_storage_class_by_path /jfs/file_case/f1 STANDARD_IA
    cat /jfs/file_case/f1 >/dev/null

    tier_set_no_err "$META_URL" --id 2 /file_case/f1
    sleep 5
    assert_tier_id /jfs/file_case/f1 2
    assert_tier_sc /jfs/file_case/f1 INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/file_case/f1 INTELLIGENT_TIERING
    cat /jfs/file_case/f1 >/dev/null

#    ./juicefs tier set "$META_URL" --id 0 /file_case/f1
#    sleep 5
#    assert_tier_id /jfs/file_case/f1 0
#    assert_object_storage_class_by_path /jfs/file_case/f1 STANDARD
#    cat /jfs/file_case/f1 >/dev/null
}

test_tier_dir_recursive_and_non_recursive()
{
    setup_tier_volume

    mkdir -p /jfs/dir_case/d1/d2
    echo "a" > /jfs/dir_case/root.txt
    echo "b" > /jfs/dir_case/d1/f1.txt
    echo "c" > /jfs/dir_case/d1/d2/f2.txt

    tier_set_no_err "$META_URL" --id 1 /dir_case
    assert_tier_id /jfs/dir_case 1
    assert_tier_id /jfs/dir_case/d1/f1.txt 0

    tier_set_no_err "$META_URL" --id 2 /dir_case -r
    assert_tier_id /jfs/dir_case 2
    assert_tier_id /jfs/dir_case/d1 2
    assert_tier_id /jfs/dir_case/d1/f1.txt 2
    assert_tier_id /jfs/dir_case/d1/d2/f2.txt 2
    assert_object_storage_class_by_path /jfs/dir_case/d1/f1.txt INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/dir_case/d1/d2/f2.txt INTELLIGENT_TIERING
}

test_tier_clone_after_dir_set()
{
    setup_tier_volume

    mkdir -p /jfs/clone_src/a/b
    for i in $(seq 1 20); do
        echo "data_$i" > /jfs/clone_src/a/b/file_$i
    done

    tier_set_no_err "$META_URL" --id 2 /clone_src -r
    ./juicefs clone /jfs/clone_src /jfs/clone_dst
    diff -ur /jfs/clone_src /jfs/clone_dst --no-dereference

    src_tier=$(get_tier_token /jfs/clone_src/a/b/file_1)
    dst_tier=$(get_tier_token /jfs/clone_dst/a/b/file_1)
    [[ "$src_tier" != "$dst_tier" ]] && echo "<FATAL>: clone tier mismatch src=$src_tier dst=$dst_tier" && exit 1
    assert_object_storage_class_by_path /jfs/clone_src/a/b/file_1 INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/clone_dst/a/b/file_1 INTELLIGENT_TIERING
}

test_tier_change_mapping_after_set()
{
    setup_tier_volume

    mkdir -p /jfs/reconf
    echo "reconf" > /jfs/reconf/file

    tier_set_no_err "$META_URL" --id 2 /reconf/file
    assert_tier_id /jfs/reconf/file 2
    assert_tier_sc /jfs/reconf/file INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/reconf/file INTELLIGENT_TIERING

    ./juicefs config "$META_URL" --tier-id 2 --tier-sc STANDARD_IA -y
    sleep 5
    assert_tier_id /jfs/reconf/file 2
    assert_tier_sc /jfs/reconf/file STANDARD_IA
    assert_object_storage_class_by_path /jfs/reconf/file STANDARD_IA
    cat /jfs/reconf/file >/dev/null
}

test_tier_invalid_mapping_reapply_recursive_should_fix_children()
{
    setup_tier_volume

    mkdir -p /jfs/invalid_map_case/a/b
    dd if=/dev/urandom of=/jfs/invalid_map_case/root.bin bs=1M count=8 status=none
    dd if=/dev/urandom of=/jfs/invalid_map_case/a/b/child.bin bs=1M count=8 status=none

    ./juicefs config "$META_URL" --tier-id 2 --tier-sc GLACIER_IR -y
    sleep 5
    tier_set_no_err "$META_URL" --id 2 /invalid_map_case -r
    assert_tier_sc /jfs/invalid_map_case GLACIER_IR
    assert_tier_sc /jfs/invalid_map_case/a GLACIER_IR
    assert_tier_sc /jfs/invalid_map_case/a/b GLACIER_IR
    assert_tier_sc /jfs/invalid_map_case/root.bin GLACIER_IR
    assert_tier_sc /jfs/invalid_map_case/a/b/child.bin GLACIER_IR
    assert_object_storage_class_by_path /jfs/invalid_map_case/root.bin GLACIER_IR
    assert_object_storage_class_by_path /jfs/invalid_map_case/a/b/child.bin GLACIER_IR

    ./juicefs config "$META_URL" --tier-id 2 --tier-sc INTELLIGENT_TIERING -y
    sleep 5
    tier_set_no_err "$META_URL" --id 2 /invalid_map_case -r
    assert_tier_sc /jfs/invalid_map_case INTELLIGENT_TIERING
    assert_tier_sc /jfs/invalid_map_case/a INTELLIGENT_TIERING
    assert_tier_sc /jfs/invalid_map_case/a/b INTELLIGENT_TIERING
    assert_tier_sc /jfs/invalid_map_case/root.bin INTELLIGENT_TIERING
    assert_tier_sc /jfs/invalid_map_case/a/b/child.bin INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/invalid_map_case/root.bin INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/invalid_map_case/a/b/child.bin INTELLIGENT_TIERING
}

test_tier_invalid_storage_class_set_should_not_change_previous_type()
{
    setup_tier_volume

    mkdir -p /jfs/invalid_set_case/dir
    dd if=/dev/urandom of=/jfs/invalid_set_case/file.bin bs=1M count=8 status=none
    dd if=/dev/urandom of=/jfs/invalid_set_case/dir/sub.bin bs=1M count=8 status=none

    tier_set_no_err "$META_URL" --id 1 /invalid_set_case/file.bin
    tier_set_no_err "$META_URL" --id 1 /invalid_set_case/dir -r
    assert_tier_sc /jfs/invalid_set_case/file.bin STANDARD_IA
    assert_tier_sc /jfs/invalid_set_case/dir STANDARD_IA
    assert_tier_sc /jfs/invalid_set_case/dir/sub.bin STANDARD_IA
    assert_object_storage_class_by_path /jfs/invalid_set_case/file.bin STANDARD_IA
    assert_object_storage_class_by_path /jfs/invalid_set_case/dir/sub.bin STANDARD_IA

    assert_config_tier_sc_fail 2 WRONG_STORAGE_CLASS
    tier_set_no_err "$META_URL" --id 2 /invalid_set_case/file.bin
    tier_set_no_err "$META_URL" --id 2 /invalid_set_case/dir -r

    assert_tier_sc /jfs/invalid_set_case/file.bin INTELLIGENT_TIERING
    assert_tier_sc /jfs/invalid_set_case/dir INTELLIGENT_TIERING
    assert_tier_sc /jfs/invalid_set_case/dir/sub.bin INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/invalid_set_case/file.bin INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/invalid_set_case/dir/sub.bin INTELLIGENT_TIERING
}

test_tier_config_change_then_set_and_overwrite_should_use_new_storage_class()
{
    setup_tier_volume

    mkdir -p /jfs/rewrite_case
    dd if=/dev/urandom of=/jfs/rewrite_case/file.bin bs=1M count=8 status=none

    ./juicefs config "$META_URL" --tier-id 2 --tier-sc INTELLIGENT_TIERING -y
    sleep 5
    tier_set_no_err "$META_URL" --id 2 /rewrite_case/file.bin
    assert_tier_id /jfs/rewrite_case/file.bin 2
    assert_tier_sc /jfs/rewrite_case/file.bin INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/rewrite_case/file.bin INTELLIGENT_TIERING

    ./juicefs config "$META_URL" --tier-id 2 --tier-sc STANDARD_IA -y
    sleep 5
    tier_set_no_err "$META_URL" --id 2 /rewrite_case/file.bin
    assert_tier_id /jfs/rewrite_case/file.bin 2
    assert_tier_sc /jfs/rewrite_case/file.bin STANDARD_IA
    assert_object_storage_class_by_path /jfs/rewrite_case/file.bin STANDARD_IA

    dd if=/dev/urandom of=/jfs/rewrite_case/file.bin bs=1M count=8 status=none
    cat /jfs/rewrite_case/file.bin >/dev/null
    assert_tier_id /jfs/rewrite_case/file.bin 2
    assert_tier_sc /jfs/rewrite_case/file.bin STANDARD_IA
    assert_object_storage_class_by_path /jfs/rewrite_case/file.bin STANDARD_IA
}

test_tier_mixed_tree_reapply_after_mapping_change()
{
    setup_tier_volume

    mkdir -p /jfs/mixed_case/dir1/dir2
    dd if=/dev/urandom of=/jfs/mixed_case/dir1/old.bin bs=1M count=8 status=none

    ./juicefs config "$META_URL" --tier-id 2 --tier-sc STANDARD_IA -y
    sleep 5
    tier_set_no_err "$META_URL" --id 2 /mixed_case -r
    assert_tier_sc /jfs/mixed_case/dir1/old.bin STANDARD_IA
    assert_object_storage_class_by_path /jfs/mixed_case/dir1/old.bin STANDARD_IA

    ./juicefs config "$META_URL" --tier-id 2 --tier-sc INTELLIGENT_TIERING -y
    sleep 5
    dd if=/dev/urandom of=/jfs/mixed_case/dir1/dir2/new.bin bs=1M count=8 status=none
    tier_set_no_err "$META_URL" --id 2 /mixed_case -r

    assert_tier_sc /jfs/mixed_case INTELLIGENT_TIERING
    assert_tier_sc /jfs/mixed_case/dir1 INTELLIGENT_TIERING
    assert_tier_sc /jfs/mixed_case/dir1/dir2 INTELLIGENT_TIERING
    assert_tier_sc /jfs/mixed_case/dir1/old.bin INTELLIGENT_TIERING
    assert_tier_sc /jfs/mixed_case/dir1/dir2/new.bin INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/mixed_case/dir1/old.bin INTELLIGENT_TIERING
    assert_object_storage_class_by_path /jfs/mixed_case/dir1/dir2/new.bin INTELLIGENT_TIERING
}

test_tier_glacier_deep_archive_restore()
{
    setup_tier_volume

    mkdir -p /jfs/archive_case/sub
    echo "archivedata1" > /jfs/archive_case/a.txt
    echo "archivedata2" > /jfs/archive_case/sub/b.txt

    ./juicefs config "$META_URL" --tier-id 3 --tier-sc GLACIER -y
    sleep 5
    tier_set_no_err "$META_URL" --id 3 /archive_case -r
    assert_tier_id /jfs/archive_case/a.txt 3
    assert_tier_sc /jfs/archive_case/a.txt GLACIER
    assert_object_storage_class_by_path /jfs/archive_case/a.txt GLACIER

    # GLACIER objects are not directly readable, so restore first.
    ./juicefs tier restore "$META_URL" /archive_case -r

    ./juicefs config "$META_URL" --tier-id 3 --tier-sc DEEP_ARCHIVE -y
    sleep 5
    tier_set_no_err "$META_URL" --id 3 /archive_case/sub/b.txt
    assert_tier_id /jfs/archive_case/sub/b.txt 3
    assert_tier_sc /jfs/archive_case/sub/b.txt DEEP_ARCHIVE
    assert_object_storage_class_by_path /jfs/archive_case/sub/b.txt DEEP_ARCHIVE

    # DEEP_ARCHIVE objects are not directly readable, so restore first.
    ./juicefs tier restore "$META_URL" /archive_case/sub/b.txt
}

init_aws_bucket
source .github/scripts/common/run_test.sh && run_test "$@"
