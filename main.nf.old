#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.chunksize = 5


process eggnog_mapper {
	input:
	path(proteins)
	// path(eggnog_db)

	output:
	path("${proteins}.emapper.annotations.txt"), emit: eggnog

	script:
	"""
	touch ${proteins}.emapper.annotations.txt
	"""
	// emapper.py -i ${genome_id}.faa --data_dir ${eggnog_db} --output ${genome_id}/${genome_id} -m diamond --cpu $task.cpus --dbmem
}



workflow {

	protein_ch = Channel.fromPath(params.input_proteins)
		.splitFasta(by: params.chunksize, file: true)

	protein_ch.view()

	eggnog_mapper(protein_ch)
	
	eggnog_mapper.out.eggnog.view()

}


