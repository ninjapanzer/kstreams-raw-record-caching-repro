#!/usr/bin/env bash
# Runs the four variants' tests and prints one comparison table.
# MAVEN_REPO=<url> resolves dependencies from that repository instead of Maven Central.
set -euo pipefail
cd "$(dirname "$0")"

repo_args=()
if [[ -n "${MAVEN_REPO:-}" ]]; then
  repo_args=("-PmavenRepo=${MAVEN_REPO}")
fi

variants=(
  "-PkafkaVersion=4.2.1"
  "-PkafkaVersion=4.0.2"
  "-PkafkaVersion=4.2.1 -Pcaching=false"
  "-PkafkaVersion=4.2.1 -Ppatched=true"
)

results=()
for variant in "${variants[@]}"; do
  echo "== ${variant}" >&2
  # shellcheck disable=SC2086
  output=$(./gradlew "${repo_args[@]+"${repo_args[@]}"}" ${variant} test 2>&1) || { echo "${output}" >&2; exit 1; }
  results+=("$(grep -o 'RESULT|.*' <<<"${output}" | tail -1)")
done

printf '\n| %-30s | %5s | %15s | %16s | %16s | %12s |\n' \
  "variant" "keys" "live raw values" "raw MB reachable" "cache counts MB" "heap MB"
printf '|%s|%s|%s|%s|%s|%s|\n' "--------------------------------" "-------" "-----------------" "------------------" "------------------" "--------------"
for result in "${results[@]}"; do
  IFS='|' read -r _ label keys live raw counted heap <<<"${result}"
  printf '| %-30s | %5s | %15s | %16s | %16s | %12s |\n' "${label}" "${keys}" "${live}" "${raw}" "${counted}" "${heap}"
done
