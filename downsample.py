'''
Author : Saideep Gona

This script downsamples fastq files
'''

import sys, os, argparse, glob
import pandas as pd

cwd = os.getcwd()
print("Python Interpreter: ", sys.executable)

parser = argparse.ArgumentParser()
parser.add_argument("input_table", help="path to readset with paths")
parser.add_argument("nlanes", help="Number of lanes samples split under")
parser.add_argument("nreads", help="Number of reads to sample to")
parser.add_argument("output_dir", help="Output directory to place downsampled files")
args = parser.parse_args()

fq_sample = "/project2/lbarreiro/users/Saideep/subsample_rnaseq/fastq-tools-0.8.3/bin/fastq-sample"

fqs_t = pd.read_csv(args.input_table, delimiter="\t")
print(fqs_t.columns)
fqs = list(fqs_t["FASTQ1"])
fqs = [x.strip() for x in fqs]
print(fqs)


samp = int(int(args.nreads)/int(args.nlanes))

for i,fq in enumerate(fqs):
    zcom = [
        # "zcat",
        "mv",
        fq,
        # ">",
        # os.path.join(args.output_dir,fq.split("/")[-1].split(".")[0]+".fastq")
        os.path.join(args.output_dir,fq.split("/")[-1].split(".")[0]+"_"+str(samp))+".fastq.gz"
        ]
    print("\n"+" ".join(zcom))
    os.system(" ".join(zcom))

    com = [
        fq_sample,
        "-n", str(samp),
        "-o", os.path.join(args.output_dir,fq.split("/")[-1].split(".")[0]+"_"+str(samp)),
        os.path.join(args.output_dir,fq.split("/")[-1].split(".")[0]+".fastq")
    ]

    print(" ".join(com))
    # os.system(" ".join(com))

# os.system("gzip " + "/project2/lbarreiro/users/Saideep/subsample_rnaseq/fastqs/*")