#!/bin/bash

# Arguments must already have the same splitting and globbing as the CLI call.
# Keep value-taking options in sync with initClientCommand in cmd/go-ycsb/client.go.
benchmark_redaction_enabled() {
    local redact=false
    local skip_value=false
    local arg

    for arg in "$@"; do
        # A value-taking option consumes the next token even if it looks like a flag.
        if [ "$skip_value" = true ]; then
            skip_value=false
            continue
        fi
        case "$arg" in
            -p|-P|--prop|--property_file|--table|--threads|--target|--interval) skip_value=true ;;
            --) break ;;
            --redact=false|--redact=False|--redact=FALSE|--redact=f|--redact=F|--redact=0) redact=false ;;
            --redact|--redact=*) redact=true ;;
        esac
    done

    [ "$redact" = true ]
}
