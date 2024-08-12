import sys, os, glob

inp_dir = sys.argv[1]

all_fqs = glob.glob(sys.argv[1]+"/*")

for fq in all_fqs:
    print(fq)
    samp = fq.split("/")[-1].replace(".fastq.gz","").replace("fastq","")
    os.system("mkdir "+os.path.join(inp_dir,samp))
    os.system("mv "+fq+" "+os.path.join(inp_dir,samp))

all_dirs = glob.glob(sys.argv[1]+"/*")

for d in all_dirs:
    cur_dir = d
    cur_fq = os.path.join(d,os.path.basename(d)+".fastq.gz")

    samp = os.path.basename(d).split("_")[0]+"_"+os.path.basename(d).split("_")[1]

    print("mkdir "+ os.path.join(inp_dir,samp))
    os.system("mkdir "+ os.path.join(inp_dir,samp))
    
    os.path.join(inp_dir,samp,cur_fq.replace("_100000",""))
    print("mv "+cur_fq+" "+os.path.join(inp_dir,samp,os.path.basename(cur_fq).replace("_100000","")))
    os.system("mv "+cur_fq+" "+os.path.join(inp_dir,samp,os.path.basename(cur_fq).replace("_100000","")))
    
    print("rm -R "+d)
    os.system("rm -R "+d)
