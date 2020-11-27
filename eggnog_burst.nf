#!/usr/bin/env nextflow

datapath = "/g/scb2/bork/mocat/freezer/prok-refdb/v11.0.0"
prefix = "prok-refdb-v11.0.0_proteins-v1"
prefix = "prok-refdb-v11.0.0_specI-v2_representatives-v2UL_proteins-v1"
input_proteins = datapath + "/" + prefix + ".faa"

chunksize = 10000
params.output_dir = "eggnog_results"

/*input_proteins = "test.faa"
prefix = "test"
chunksize = 10
params.output_dir = "eggnog_test_results"*/


Channel
	.fromPath(file(input_proteins))
	.splitFasta(by: chunksize, file: prefix)
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
result_run_eggnog_mapper.view { it }

/*eggnog_chunks_sorted_ch
	.toSortedList()
	.set { eggnog_chunks_sorted_ch }*/

eggnog_chunks_ch
	.collectFile(name: params.output_dir + "/" + prefix + ".emapper.annotations", newLine: true)
//	.subscribe onNext: { print "collecting file $it" }, onComplete: "Done."
//		println "Collecting eggnog files into: $it"
//	}
