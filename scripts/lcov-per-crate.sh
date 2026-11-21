#!/bin/sh
# Split a workspace-relative lcov file into per-crate reports with
# crate-relative paths.
#
# Sonar's Rust LCOV sensor resolves reportPaths against each module's base
# directory (one module per workspace crate). This repository's root is a
# pure virtual manifest, so no sensor runs at the root and a single
# root-level report is never found. Splitting gives every crate module its
# own report with paths relative to that crate.
# Portable across GNU/BSD awk.
set -eu
in=${1:?usage: lcov-per-crate.sh <workspace.info> <basename>}
base=${2:?usage: lcov-per-crate.sh <workspace.info> <basename>}
for d in crates/*/; do
    name=${d%/}
    name=${name#crates/}
    awk -v prefix="SF:crates/${name}/" '
        /^SF:/ { keep = index($0, prefix) == 1 }
        keep {
            if (/^SF:/) { $0 = "SF:" substr($0, length(prefix) + 1) }
            print
        }
        /^end_of_record$/ { keep = 0 }
    ' "$in" > "crates/${name}/${base}"
done
