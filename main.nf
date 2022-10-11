#!/usr/bin/env nextflow

params.chunksize = 5

workflow {

	protein_ch = Channel.fromPath(params.input_proteins)
		.splitFasta(by: params.chunksize, file: true)

	protein_ch.view()

}


