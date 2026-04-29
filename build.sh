#!/usr/bin/env bash
set -e

OUTPUT="talaria"
echo "Building $OUTPUT..."
go build -o "$OUTPUT" .
echo "Done: ./$OUTPUT"
