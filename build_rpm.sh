#!/usr/bin/env bash
set -euxo pipefail
cd "$(dirname "$0")"
export LLVM_CONFIG_PATH=/usr/bin/llvm-config
export LIBCLANG_PATH=/usr/lib64
NAME="${NAME:-rocks-wal-tailer}"
VERSION="${1:-$(awk -F\" '/^version[[:space:]]*=/ {print $2; exit}' Cargo.toml)}"
TARBALL="${NAME}-${VERSION}.tar.gz"
echo $VERSION
# Where the spec actually is
SPEC_PATH="${SPEC_PATH:-packaging/${NAME}.spec}"
[[ -f "$SPEC_PATH" ]] || SPEC_PATH="./${NAME}.spec"  # fallback

# Silence git “dubious ownership” if present, but work without git too
git config --global --add safe.directory "$(pwd)" >/dev/null 2>&1 || true
if git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  git ls-files -z | tar --null -czf "/tmp/${TARBALL}" \
    --transform "s,^,${NAME}-${VERSION}/," -T -
else
  tar -czf "/tmp/${TARBALL}" \
    --exclude=target --exclude=.git --exclude=.github \
    --transform "s,^,${NAME}-${VERSION}/," .
fi

RPMTOP="${RPMTOP:-$HOME/rpmbuild}"
mkdir -p "$RPMTOP"/{SOURCES,SPECS,BUILD,RPMS,SRPMS}

# Copy source tarball and spec
cp "/tmp/${TARBALL}" "$RPMTOP/SOURCES/"
cp -f "$SPEC_PATH" "$RPMTOP/SPECS/"

# Also copy packaging assets the spec may reference (as Source1/2/3…)
for f in packaging/systemd/* packaging/sysconfig/* packaging/tmpfiles.d/*; do
  [[ -e "$f" ]] && cp -f "$f" "$RPMTOP/SOURCES/"
done

rpmbuild -ba "$RPMTOP/SPECS/${NAME}.spec"

echo
echo "Built RPMs:"
find "$RPMTOP/RPMS" -type f -name '*.rpm' -printf '  %p\n'

OUT="/src/dist"
mkdir -p "$OUT"
cp -v "$RPMTOP"/RPMS/*/*.rpm "$OUT"/ 2>/dev/null || true
cp -v "$RPMTOP"/SRPMS/*.src.rpm "$OUT"/ 2>/dev/null || true
echo "Copied artifacts to $OUT"
