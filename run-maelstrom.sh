#!/usr/bin/env bash


export PATH="/opt/homebrew/opt/openjdk/bin:$PATH"
export PATH="/usr/local/opt/openjdk/bin:$PATH"

~/workspace/maelstrom/maelstrom "$@"
