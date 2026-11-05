#!/bin/sh
# Builds inst/proto/spark-connect.binpb, the descriptor set zuspark encodes
# requests and decodes responses with (design §5, D4), and records what it
# was built from in inst/proto/VERSION. Run from the package root:
#
#   sh tools/build-descriptors.sh
#
# It needs curl and protoc, and runs only when a pin below changes, never at
# install time. Started from zugrpc's tools/gen-spark-descriptors.sh. Spark
# is pinned to the tag of the Docker image the live tests run (design Q1,
# decided 2026-11-05); re-pinning is a stage-less pull request that re-runs
# the proto snapshots. The SHA-256 in VERSION is checked by the tests.
set -eu

SPARK_TAG=v4.2.1
SPARK_FILES="base catalog commands common example_plugins expressions ml ml_common pipelines relations types"
SPARK_SRC=sql/connect/common/src/main/protobuf
GOOGLEAPIS_COMMIT=02362883cd16428e5f57fa60ca8d8f60dafcdba7
GOOGLE_FILES="google/rpc/status.proto google/rpc/error_details.proto"
OUT=inst/proto/spark-connect.binpb

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/spark/connect" "$tmp/google/rpc" "$(dirname "$OUT")"

protos=""
for f in $SPARK_FILES; do
  curl -fsSL -o "$tmp/spark/connect/$f.proto" \
    "https://raw.githubusercontent.com/apache/spark/$SPARK_TAG/$SPARK_SRC/spark/connect/$f.proto"
  protos="$protos spark/connect/$f.proto"
done
for f in $GOOGLE_FILES; do
  curl -fsSL -o "$tmp/$f" \
    "https://raw.githubusercontent.com/googleapis/googleapis/$GOOGLEAPIS_COMMIT/$f"
  protos="$protos $f"
done
curl -fsSL -o inst/proto/NOTICE \
  "https://raw.githubusercontent.com/apache/spark/$SPARK_TAG/NOTICE"

# No --include_imports: the only imports outside this set are well-known
# types, which every zuproto pool already holds.
(cd "$tmp" && protoc --proto_path=. --descriptor_set_out=out.binpb $protos)
cp "$tmp/out.binpb" "$OUT"

if command -v sha256sum >/dev/null 2>&1; then
  sum=$(sha256sum "$OUT" | cut -d' ' -f1)
else
  sum=$(shasum -a 256 "$OUT" | cut -d' ' -f1)
fi

cat > inst/proto/VERSION <<EOF
spark: $SPARK_TAG
googleapis: $GOOGLEAPIS_COMMIT
protoc: $(protoc --version | sed 's/^libprotoc //')
sha256: $sum
EOF
echo "wrote $OUT ($sum), Spark $SPARK_TAG, googleapis $GOOGLEAPIS_COMMIT"
