#!/usr/bin/env nextflow

nextflow.enable.dsl=2

params.chunksize = 20000



process run_emapper {
    container "oras://ghcr.io/cschu/profile_me_ci@sha256-d4c01a50720b65dfecb1d8769cfbcb17fb444d0398960cd305395d8da21fd43a"

    input:
    path proteins
    path eggnog_db

    output:
    path "emapper/*.emapper.annotations", emit: annotations
    path "emapper/*.emapper.seed_orthologs", emit: orthologs

    script:
    """
    mkdir -p emapper/ tmp/
    emapper.py -i ${proteins} --data_dir ${eggnog_db} --output emapper/{proteins} -m diamond --cpu ${task.cpus} --dbmem --temp_dir tmp/
    """

}


process merge_emapper_output {
    publishDir "${params.output_dir}", mode: "copy"

    input:
    path annotations

    output:
    path "emapper_merged/emapper_annotations.tsv.gz"

    script:
    """
    mkdir -p emapper_merged/

    head -n 4 ${annotations[0]} | gzip -c - > emapper_merged/emapper_annotations.tsv.gz
    grep -v '#' ${annotations} | cut -f 2- -d : | gzip -c - > emapper_merged/emapper_annotations.tsv.gz
    """
}




workflow {

    proteins_ch = Channel.fromPath(params.input_proteins, checkIfExists: true)
        .splitFasta(by: params.chunksize, file: true)

    run_emapper(proteins_ch, params.eggnog_db)

    merge_emapper_output(run_emapper.out.annotations.collect())

}
