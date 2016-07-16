#!/usr/bin/env bash

set -euo pipefail

benchmarks=("BenchmarkInsert" "BenchmarkFastInsert" "BenchmarkGet" "BenchmarkDelete")
degrees=(2 4 8 16 32 64 128 256)

for b in ${benchmarks[@]}; do
  for d in ${degrees[@]}; do
    printf "benchmark: %s, degree: %d\n" $b $d
    go test -run $b -bench $b -degree $d
  done
done
