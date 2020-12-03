#!/usr/bin/env nextflow
// https://github.com/erikrikarddaniel/pfitmap-nextflow/blob/master/main.nf <- source for how to do certain things

/*
datapath = "/g/scb2/bork/mocat/freezer/prok-refdb/v11.0.0"
prefix = "prok-refdb-v11.0.0_proteins-v1"
prefix = "prok-refdb-v11.0.0_specI-v2_representatives-v2UL_proteins-v1"
input_proteins = datapath + "/" + prefix + ".faa"

chunksize = 20000
params.output_dir = "eggnog_results"
*/

/*
input_proteins = "test.faa"
prefix = "test"
input_proteins = "prok-refdb-v11.0.0_specI-v2_representatives-v2UL_proteins-v1_head.faa"
prefix = "prok-refdb-v11.0.0_specI-v2_representatives-v2UL_proteins-v1_head"

chunksize = 10
params.output_dir = "eggnog_test_results"
*/

def helpMessage() {
	log.info """


	This is the eggnog_burst pipeline for adding eggNOG functional annotation to a gene set.

	Usage:

	The typical command for running the pipeline is as follows:

		nextflow run -C run.config eggnog_burst.nf --input_proteins <input_proteins> --db <path_to_gene_db> [--output_dir <output_dir>] [--chunksize <chunksize>]

		Mandatory arguments:

			--input_proteins		Path to fasta file with input protein sequences
			--db					Path to sqlite database with gene information

		Optional arguments:

			--output_dir			Path to output directory (default: 'eggnog_burst_results')
			--chunksize				Size of protein subsets for parallel eggNOG-mapping (default: 20000)

			--help					Display this help

		Configuration:

			Modify run.config params section to set paths to required resources!

	""".stripIndent()
}

if (params.help) {
	helpMessage()
	exit 0
}

// TODO: add checks for mandatory args

if ( !params.output_dir ) {
	params.output_dir = "eggnog_burst_results"
}

if ( !params.chunksize ) {
	params.chunksize = 20000
}

// https://stackoverflow.com/questions/1569547/does-groovy-have-an-easy-way-to-get-a-filename-without-the-extension
prefix = params.input_proteins - ~/\.\w+$/
// mine.
prefix = prefix - ~/\/?([^\/]+\/)+/

log.info "${prefix}"
//exit 0

gene_db = file(params.db)


Channel
	.fromPath(params.input_proteins, checkIfExists: true)
	.splitFasta(by: params.chunksize, file: true)
	.set { chunks_ch }

process run_eggnog_mapper {
    conda "bioconda::eggnog-mapper"
    publishDir "$params.output_dir/eggNOG"
	errorStrategy "retry"
	maxRetries 3


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

process merge_eggnog_output {
	publishDir "$params.output_dir"

	input:
	file chunk from eggnog_chunks_ch.collect()

	output:
	stdout result_merge_eggnog_output
	file "${prefix}.emapper.annotations" into eggnog_annotation_ch

	script:
	"""
	ls ${chunk} | awk -v prefix=$prefix -v OFS='\t' '{ idx=gensub(prefix, "", "g", \$1); split(idx, arr, "."); print arr[2],\$1}' | sort -k1,1g | cut -f 2 | xargs cat > ${prefix}.emapper.annotations.1
	head -n4 ${prefix}.emapper.annotations.1 > ${prefix}.emapper.annotations
	grep -v '#' ${prefix}.emapper.annotations.1 >> ${prefix}.emapper.annotations
	rm ${prefix}.emapper.annotations.1
	"""
}

process combine_annotations {
	publishDir "$params.output_dir"

	input:
	file eggnog_annotation from eggnog_annotation_ch

	output:
	stdout result_combine_annotations
	file "${prefix}.gff"

	script:
	"""
	python ${params.script_dir}/annotate_gff.py ${gene_db} ${eggnog_annotation}
	echo "done"
	"""
}
result_combine_annotations.view { it }
