#!/bin/sh
set -eu

if [ -z "${TEST_CHANNEL_SECRET:-}" ] && [ -n "${MESHCORE_CHANNEL_SECRETS:-}" ]; then
  wanted_name=$(printf '%s' "${TEST_CHANNEL_NAME:-health-check}" | tr '[:upper:]' '[:lower:]')
  secret_source_name=$(printf '%s' "${TEST_CHANNEL_SECRET_SOURCE_NAME:-}" | tr '[:upper:]' '[:lower:]')
  old_ifs=$IFS
  IFS=,

  for channel_entry in $MESHCORE_CHANNEL_SECRETS; do
    channel_entry=$(printf '%s' "$channel_entry" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')
    [ -n "$channel_entry" ] || continue

    case "$channel_entry" in
      *:*)
        channel_name=${channel_entry%%:*}
        channel_secret=${channel_entry#*:}
        ;;
      *)
        channel_name=${channel_entry}
        channel_secret=${channel_entry}
        ;;
    esac

    normalized_name=$(printf '%s' "$channel_name" | tr '[:upper:]' '[:lower:]')
    if [ "$normalized_name" = "$wanted_name" ] || { [ -n "$secret_source_name" ] && [ "$normalized_name" = "$secret_source_name" ]; }; then
      export TEST_CHANNEL_SECRET=$channel_secret
      break
    fi
  done

  IFS=$old_ifs
fi

exec "$@"
