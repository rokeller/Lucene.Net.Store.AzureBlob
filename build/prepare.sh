#!/bin/bash

LONG_TAG=$(git describe --long)
VER_MAJORMINOR=$(echo $LONG_TAG | sed -r 's|^v?([0-9.]+)-[0-9]+-g[0-9a-f]+$|\1|g')
VER_MAJOR=$(echo $VER_MAJORMINOR | sed -r 's|^([0-9]+)\.[0-9]+$|\1|g')
VER_COMMITS_SINCE_TAG=$(echo $LONG_TAG | sed -r 's|^v?[0-9.]+-([0-9]+)-g[0-9a-f]+$|\1|g')

if [ "$CI_COMMIT_REF_NAME" = "master" ]; then
    FLAVOR=Release
    BRANCH_SUFFIX=
else
    FLAVOR=Debug
    BRANCH_SUFFIX=$CI_COMMIT_REF_NAME
fi

VERSION_BRANCH_SUFFIX=$(echo $BRANCH_SUFFIX | sed -r 's|/|-|g')

VERSION_BASE=$VER_MAJORMINOR.$VER_COMMITS_SINCE_TAG
VERSION_ASSEMBLY=$VER_MAJOR.0.0.0
VERSION_PRODUCT=$VERSION_BASE.0

BUILD_ARGS="-p:AssemblyVersion=$VERSION_ASSEMBLY -p:VersionPrefix=$VERSION_PRODUCT -p:VersionSuffix=$VERSION_BRANCH_SUFFIX"
BUILD_ARGS="$BUILD_ARGS -p:RepositoryBranch=$BRANCH_SUFFIX -p:RepositoryUrl=$CI_PROJECT_URL -p:PackageProjectUrl=$CI_PROJECT_URL"

TEST_ARGS="--no-build -p:CollectCoverage=true -p:CoverletOutputFormat=\"lcov%2copencover\" -p:CoverletOutput=../../TestResults/"

export CONNECTION_STRING="AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;DefaultEndpointsProtocol=http;BlobEndpoint=http://azurite:10000/devstoreaccount1;QueueEndpoint=http://azurite:10001/devstoreaccount1;TableEndpoint=http://azurite:10002/devstoreaccount1;"

echo "FLAVOR    : $FLAVOR"
echo "BUILD_ARGS: $BUILD_ARGS"
echo "TEST_ARGS : $TEST_ARGS"