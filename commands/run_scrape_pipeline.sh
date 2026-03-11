#!/bin/bash

# Default fallback values
project_id="unknown"
instance_id="unknown"
uv_command="uv"

# Parse the arguments passed by the scheduler
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --project) project_id="$2"; shift ;;
        --instance) instance_id="$2"; shift ;;
        --uv) uv_command="$2"; shift ;;
        *) echo "Unknown parameter passed: $1"; exit 1 ;;
    esac
    shift
done

cd ~/Apps/hermes/
$uv_command run scripts/scrape_pipeline.py
