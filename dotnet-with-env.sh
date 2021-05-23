#!/bin/bash

if [ -f './.env' ]; then
    echo Applying .env file
    set -a
    source './.env'
fi

echo Running dotnet with
echo "$@"
dotnet "$@"

echo dotnet command exit code: $?
