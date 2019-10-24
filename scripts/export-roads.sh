#!/bin/bash
set -eu

function usage() {
    echo -n \
"Usage: $(basename "$0") <geojson_input> <s3_output_directory>
Export vector tiles which represent the road network inputs used during serve/unserved population calculations
"
}

if [ "${BASH_SOURCE[0]}" = "${0}" ]
then
    if [ "${1:-}" = "--help" ]
    then
        usage
    elif [ -z ${1+x} ]
    then
      usage
    elif [ -z ${2+x} ]
    then
      usage
    else
      GEOJSON_INPUT=$1
      S3_OUTPUT_DIR=$2

      docker run --rm \
        -e "AWS_PROFILE=geotrellis" \
        -v $GEOJSON_INPUT:/data/input.geojson \
        -v $HOME/.aws:/home/tilertwo/.aws:ro \
        quay.io/moradology/tilertwo_tiler-two:latest \
        file:///data/input.geojson \
        $S3_OUTPUT_DIR \
        --tippecanoe-opts "-l road-network --drop-densest-as-needed -zg --hilbert"
    fi
    exit
fi

