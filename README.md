
This is a luigi implementation of the GATK RNAseq genotyping pipeline for the UChicago 
Midway2 Cluster

***** DESCRIPTION(END) *************

***** INSTALLATION *************

module load python
pip install luigi   OR   conda install -c anaconda luigi

***** INSTALLATION(END) *************

***** Current optimal run scheme:   *************
    1st run (First Three Steps)
        python /project2/lbarreiro/programs/rnaseq-variant-pipeline/run_parallel.py 
        \ <experiment-id> 
        \--partition bigmem2(or lbarreiro) --memory 100G --task_until AlignStep2Sample
    2nd run (Remaining Steps)
        python /project2/lbarreiro/programs/rnaseq-variant-pipeline/run_parallel.py 
        \ <experiment-id> 
        \--partition broadwl --memory 32G --task_until RenameVCF     
    Generate Heatmap (Run Once Individual Files Are Processed. SNPRelate R package is required)
        python /project2/lbarreiro/programs/rnaseq-variant-pipeline/run_parallel.py 
        \ <experiment-id>
        \ --partition broadwl --memory 32G --task_until ProduceHeatmap

        ------OR (For QUICK IDENTIFICATION) -------
    
        python /project2/lbarreiro/programs/rnaseq-variant-pipeline/run_parallel.py 
        \ <experiment-id>
        \ --partition broadwl --memory 32G --task_until ProduceHeatmapQUICK
***** Current optimal run scheme (END):   *************


***** FULL INSTRUCTIONS *************

Parallelizes the GATK rnaseq variant calling pipeline by sample.

1.) Set up your directories as follows:

output_directory/ <- Specified in config file. All pipeline outputs will go here
        |
        data/     <- Will automatically check here for input data
            | 
            experiment_id1/      
                |
                sample_id1
                    |
                    *.fastq.gz
                    *.fastq.gz
                sample_id2
                    |
                    *.fastq.gz
                    *.fastq.gz
                ...
            experiment_id2/
                |
                sample_id1
                    |
                    *.fastq.gz
                    *.fastq.gz
                sample_id2
                    |
                    *.fastq.gz
                    *.fastq.gz
                ...
NOTE: Helper script create_single_dir.py is provided for quickly creating sample directories for single fastq samples

2.) Before running this script, create a "luigi.cfg" file. Template: ./luigi_example.cfg

3.) Place the "luigi.cfg" file in the "output_directory" specified above  

4.) Navigate to the "output_directory" : cd $output_directory

5.) Call the commands specified in the "Current optimal run scheme" section above. Do them in order,
waiting until the previous step has completed.

6.) The final VCF for joint calling is: 
    [output_dir]/joint_calling/[self.experiment_id]/joint.removed.final.vcf.gz

# NOTES

NOTE: If you are just interested in sample identification, downsampling reads can help improve speed

NOTE: The "experiment_id" is whatever you choose, but must match the directory structure above

NOTE: If your purpose is simply to verify the identity of the samples, you can just use:
ProduceHeatmapQUICK for the final submission. If you want to do detailed joint-genotyping,
use ProduceHeatmap

NOTE: If running joint genotyping on a large amount of individuals (100+), the joint portion of this pipeline
may be slow. If you anticipate this, let me know and we can parallelize
