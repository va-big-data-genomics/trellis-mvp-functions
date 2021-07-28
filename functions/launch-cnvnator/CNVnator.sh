#!/bin/bash 
#CNVnator BASH Script

# https://github.com/DecodeGenetics/graphtyper/issues/57
tar xzf ${REF_CACHE_SOURCE}
export REF_PATH="$(pwd)/ref/cache/%2s/%2s/%s:http://www.ebi.ac.uk/ena/cram/md5/%s"
export REF_CACHE="$(pwd)/ref/cache/%2s/%2s/%s"

cnvnator -unique -root ${ROOT} -tree ${BAM} -chrom $(seq -f 'chr%g' 1 22) chrX chrY chrM 
cnvnator -root ${ROOT} -his ${BIN_SIZE} -d ${DIR} 
cnvnator -root ${ROOT} -stat ${BIN_SIZE} 
cnvnator -root ${ROOT} -eval ${BIN_SIZE} > ${EVAL_OUT} 
cnvnator -root ${ROOT} -partition ${BIN_SIZE} 
cnvnator -root ${ROOT} -call ${BIN_SIZE} > ${CALL_OUT} 
perl /app/CNVnator_v0.4.1/src/cnvnator2VCF.pl ${CALL_OUT} ${DIR} > ${CALL_VCF} 
awk '{print $2} END {print "exit"}' ${CALL_OUT} | cnvnator -root ${ROOT} -genotype ${BIN_SIZE} > ${GENOTYPE_OUT}