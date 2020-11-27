#!/usr/bin/env nextflow

datapath = "/g/scb2/bork/mocat/freezer/prok-refdb/v11.0.0"
prefix = "prok-refdb-v11.0.0_proteins-v1"
prefix = "prok-refdb-v11.0.0_specI-v2_representatives-v2UL_proteins-v1"
input_proteins = datapath + "/" + prefix + ".faa"

chunksize = 10000
params.output_dir = "eggnog_results"

/* 
input_proteins = "test.faa"
prefix = "test"
input_proteins = "prok-refdb-v11.0.0_specI-v2_representatives-v2UL_proteins-v1_head.faa"
prefix = "prok-refdb-v11.0.0_specI-v2_representatives-v2UL_proteins-v1_head"

chunksize = 10
params.output_dir = "eggnog_test_results"
*/

Channel
	.fromPath(file(input_proteins))
	.splitFasta(by: chunksize, file: true)
	.set { chunks_ch }

process run_eggnog_mapper {
    conda "bioconda::eggnog-mapper"
    publishDir "$params.output_dir/eggNOG"

    input:
	file chunk from chunks_ch

    output:
    stdout result_run_eggnog_mapper
	file "${chunk}.emapper.annotations" into eggnog_chunks_ch

    script:
    """
    emapper.py -i ${chunk} --data_dir ${params.eggnog_db} --output ${chunk} -m diamond --cpu 8
    """
}

process combine_eggnog_output {
	publishDir "$params.output_dir"

	input:
	file chunk from eggnog_chunks_ch.collect()

	output:
	stdout result_combine_eggnog_output
	file "${prefix}.emapper.annotations"

	script:
	"""
	ls ${chunk} | awk -v prefix=$prefix -v OFS='\t' '{ idx=gensub(prefix, "", "g", \$1); split(idx, arr, "."); print arr[2],\$1}' | sort -k1,1g | cut -f 2 | xargs cat > ${prefix}.emapper.annotations.1
	head -n3 ${prefix}.emapper.annotations.1 > ${prefix}.emapper.annotations
	grep -v '#' ${prefix}.emapper.annotations.1 >> ${prefix}.emapper.annotations
	rm ${prefix}.emapper.annotations.1
	echo "done"
	"""
}
result_combine_eggnog_output.view { it }
