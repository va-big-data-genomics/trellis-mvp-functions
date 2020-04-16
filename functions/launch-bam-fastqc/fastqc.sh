#!/bin/bash 

FILEPATH_NO_EXT="${INPUT%.*}"
FILEDIR="$(dirname ${INPUT})"
OUTPUT_DIR="$(dirname ${OUTPUT})"

echo ${INPUT}
echo ${OUTPUT}
echo ${FILEPATH_NO_EXT}
echo ${OUTPUT_DIR}

fastqc ${INPUT}
unzip -d ${FILEDIR} ${FILEPATH_NO_EXT}_fastqc.zip
mv ${FILEPATH_NO_EXT}_fastqc/fastqc_data.txt ${OUTPUT}
#OUTPUT=${INPUT}.fastqc_data.txt