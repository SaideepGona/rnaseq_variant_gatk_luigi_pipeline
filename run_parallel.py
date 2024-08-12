'''
Author: Saideep Gona
'''


from glob import glob
import os
import sys
from os.path import join
import argparse

# sys.path.append('/project2/lbarreiro/programs/luigi/')

import luigi
from luigi.contrib.external_program import ExternalProgramTask
from luigi.util import requires

"""
***** DESCRIPTION *************

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

***** FULL INSTRUCTIONS (END)*************
"""

parser = argparse.ArgumentParser()
# parser.add_argument("config_file", help="Luigi config file")
parser.add_argument("experiment_id", help="Experiment id to be processed")
parser.add_argument("--partition", help="Cluster partition to be used. Default is broadwl", default="broadwl")
parser.add_argument("--memory", help="Amount of memory to allocate. Default is 32G", default="32G")
parser.add_argument("--task_until", help="Pipeline task to run until. Default to FilterVariants.", default="FilterVariants")
args = parser.parse_args()

# print(os.environ["LUIGI_CONFIG_PATH"])

# os.system("LUIGI_CONFIG_PATH="+args.config_file)
pipeline_root = "/project2/lbarreiro/programs/rnaseq-variant-pipeline/"
# os.environ["LUIGI_CONFIG_PATH"] = args.config_file

# os.environ["LUIGI_CONFIG_PATH"] = os.path.dirname(args.config_file) + "/"
# os.system("cp " + args.config_file + " " + pipeline_root + "/luigi.cfg")
# os.system("chmod 777 " + pipeline_root + "/luigi.cfg")

class rnaseq_variant_pipeline(luigi.Config):
    star_bin = luigi.Parameter()
    gatk_bin = luigi.Parameter()

    genome = luigi.Parameter()
    annotations = luigi.Parameter()
    star_index_dir = luigi.Parameter()

    output_dir = luigi.Parameter()
    tmp_dir = luigi.Parameter()

cfg = rnaseq_variant_pipeline()

def run_job(params, modules, command, sample, out_dir):
    script = "#!/bin/bash\n\n"
    script+="\n".join(["#SBATCH "+x for x in params])
    script+="\n"
    script+="\n".join(["module load "+x for x in modules])
    script+="\n"
    script+=" ".join(command) + "\n"
    with open(sample + ".sbatch", "w") as sb:
        sb.write(script)
    os.system("sbatch " + sample + ".sbatch")
    os.system("mv " + sample + ".sbatch " + out_dir + "/" + sample + ".sbatch")

outlogs = cfg.output_dir + "/outlogs/"

os.system("mkdir " + outlogs)

samples = glob(join(cfg.output_dir, 'data', args.experiment_id, '*'))
print("Looking for samples in: ",join(cfg.output_dir, 'data', args.experiment_id, '*'))

merge_tasks = {
    "MergeVCFs",
    "NormVCF",
    "MAF_Filter",
    "ProduceHeatmap",
    "CombineGVCFs",
    "JointCall",
    "MergeVCFsQUICK",
    "NormVCFQUICK",
    "MAF_FilterQUICK",
    "ProduceHeatmapQUICK",
    "CombineGVCFsQUICK",
    "JointCallQUICK"
}

if args.task_until in merge_tasks:
    samples = ['merged']

for sample in samples:
    print(sample)
    param_base = [
        # "--job-name=rnasaq-variant-" + sample.split("/")[-1],
        "--job-name=rnasaq-variant",
        "--time=8:00:00",
        "--nodes=1",
        "--ntasks=1",
        "--cpus-per-task=8",
        "--mem="+args.memory,
        "--partition="+args.partition,
        "--output="+outlogs + "rnaseq-variant-" + args.experiment_id + "_" + os.path.basename(sample) + "_" + "%J.o",
        "--error="+outlogs + "rnaseq-variant-" + args.experiment_id + "_" + os.path.basename(sample) + "_" + "%J.e",
    ]
    modules = ["STAR", "picard", "java", "bcftools/1.9", "htslib/1.4.1", "plink/1.90b6.9", "R", "python"]
    command = ["bash",
                pipeline_root+"/luigi-wrapper",
                args.task_until,
                "--experiment-id", args.experiment_id,
                "--sample-id", os.path.basename(sample),
                "--local-scheduler"
            ]

    if args.task_until in merge_tasks:
        command = ["bash",
                    pipeline_root+"/luigi-wrapper",
                    args.task_until,
                    "--experiment-id", args.experiment_id,
                    "--local-scheduler"
                ]
    print(command)
    run_job(param_base,modules,command,os.path.basename(sample),outlogs)