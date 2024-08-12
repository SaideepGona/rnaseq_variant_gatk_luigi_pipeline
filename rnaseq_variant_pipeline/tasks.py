from glob import glob
import os
from os.path import join
import sys
import gzip

sys.path.append('/project2/lbarreiro/programs/luigi/')

import luigi
from luigi.contrib.external_program import ExternalProgramTask
from luigi.util import requires

"""
This pipeline largely based on GATK 3 guidelines for RNA-Seq variant calling
adapted for GATK 4.

Reference: https://software.broadinstitute.org/gatk/documentation/article.php?id=3891
"""

class rnaseq_variant_pipeline(luigi.Config):
    star_bin = luigi.Parameter()
    gatk_bin = luigi.Parameter()

    genome = luigi.Parameter()
    annotations = luigi.Parameter()
    star_index_dir = luigi.Parameter()
    intervals = luigi.Parameter()

    output_dir = luigi.Parameter()
    tmp_dir = luigi.Parameter()

cfg = rnaseq_variant_pipeline()
tmp_dir = os.environ["SLURM_TMPDIR"]
# STAR_tmp = "$SLURM_TMPDIR"
STAR_tmp = os.environ["SLURM_TMPDIR"]

class ProduceGenome(luigi.Task):

    experiment_id = luigi.Parameter()

    """
    Produce a reference genome sequence.
    """
    def output(self):
        return luigi.LocalTarget(cfg.genome)

@requires(ProduceGenome)
class ProduceGenomeDict(ExternalProgramTask):
    """
    Produce a sequence dictionnary to query efficiently a genome.
    """

    experiment_id = luigi.Parameter()

    def program_args(self):
        return [cfg.gatk_bin, 'CreateSequenceDictionary', '-R', self.input().path, '-O', self.output().path]

    def output(self):
        return luigi.LocalTarget(os.path.splitext(self.input().path)[0] + '.dict')

class ProduceAnnotations(luigi.Task):
    """
    Produce a reference annotations.
    """
    experiment_id = luigi.Parameter()
    def output(self):
        return luigi.LocalTarget(cfg.annotations)

@requires(ProduceGenome, ProduceAnnotations)
class ProduceReference(ExternalProgramTask):
    """
    Produce a reference.
    TODO: generate the reference
    """

    experiment_id = luigi.Parameter()

    # resources = {'cpu': 8, 'mem': 32}

    def program_args(self):
        return [cfg.star_bin,
                '--runThreadN', "8",
                '--runMode', 'genomeGenerate',
                '--genomeDir', os.path.dirname(self.output().path),
                '--genomeFastaFiles', self.input()[0].path,
                '--sjdbGTFfile', self.input()[1].path,
                "--limitGenomeGenerateRAM", "102706365824",
                "--outTmpDir", STAR_tmp + "/1"]

    def run(self):
        self.output().makedirs()
        return super(ProduceReference, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.star_index_dir, 'Genome'))

class ProduceSampleFastqs(luigi.Task):
    """
    Produce the FASTQs that relate to a sample.
    """
    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()
    def output(self):
        return [luigi.LocalTarget(f) for f in sorted(glob(join(cfg.output_dir, 'data', self.experiment_id, self.sample_id, '*.fastq.gz')))]

@requires(ProduceReference, ProduceSampleFastqs)
class AlignSample(ExternalProgramTask):
    """
    Align a sample on a reference.
    """

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    # resources = {'cpu': 8, 'mem': 32}

    def program_args(self):
        args = [cfg.star_bin,
                '--genomeDir', os.path.dirname(self.input()[0].path),
                '--outFileNamePrefix', os.path.dirname(self.output().path) + '/',
                '--outSAMtype', 'BAM', 'SortedByCoordinate',
                '--runThreadN', "8",
                # FIXME: '--readStrand', 'Forward',
                '--readFilesCommand', 'zcat',
                "--outTmpDir", STAR_tmp + "/2"]

        args.append('--readFilesIn')
        # args.extend(fastq.path for fastq in self.input()[1]) # FOR SINGLE FILE PAIRED END MATES
        args.append(",".join([fastq.path for fastq in self.input()[1]])) # FOR MULTIPLE SINGLE-END READS

        return args

    def run(self):
        self.output().makedirs()
        return super(AlignSample, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'aligned', self.experiment_id, self.sample_id, 'Log.final.out'))

@requires(AlignSample)
class PrepareSampleReference(ExternalProgramTask):
    """
    Prepare a personalized reference for the sample.
    """

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    def program_args(self):
        return [cfg.star_bin,
                '--runMode', 'genomeGenerate',
                '--genomeDir', os.path.dirname(self.output().path),
                '--genomeFastaFiles', cfg.genome,
                '--sjdbFileChrStartEnd', join(os.path.dirname(self.input().path), 'SJ.out.tab'),
                '--sjdbOverhang', 75,
                '--runThreadN', "8",
                "--limitGenomeGenerateRAM", "102706365824",
                "--outTmpDir", STAR_tmp + "/3"]

    def run(self):
        self.output().makedirs()
        return super(PrepareSampleReference, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'aligned-reference', self.experiment_id, self.sample_id, 'Genome'))

# 2 PASS ALIGNMENT (https://academic.oup.com/bioinformatics/article/32/1/43/1744001)

@requires(PrepareSampleReference, ProduceSampleFastqs)
class AlignStep2Sample(ExternalProgramTask):
    """
    Perform an alignment (2-step)
    """

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    # resources = {'cpu': 8, 'mem': 32}

    def program_args(self):
        args = [cfg.star_bin,
                '--genomeDir', os.path.dirname(self.input()[0].path),
                '--outFileNamePrefix', os.path.dirname(self.output().path) + '/',
                '--outSAMtype', 'BAM', 'SortedByCoordinate',
                '--runThreadN', "8",
                # FIXME: '--readStrand', 'Forward',
                '--readFilesCommand', 'zcat',
                "--outTmpDir", STAR_tmp + "/4"]

        args.append('--readFilesIn')
        # args.extend(fastq.path for fastq in self.input()[1]) # FOR SINGLE FILE PAIRED END MATES
        args.append(",".join([fastq.path for fastq in self.input()[1]])) # FOR MULTIPLE SINGLE-END READS

        return args

    def run(self):
        self.output().makedirs()
        return super(AlignStep2Sample, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'aligned-step2', self.experiment_id, self.sample_id, 'Aligned.sortedByCoord.out.bam'))

@requires(AlignStep2Sample)
class AddOrReplaceReadGroups(ExternalProgramTask):

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    # resources = {'cpu': 1, 'mem': 24}

    def program_args(self):
        return [cfg.gatk_bin,
                'AddOrReplaceReadGroups',
                '-I', self.input().path,
                '-O', self.output().path,
                '-SO', 'coordinate',
                '--RGID', 'id',
                '--RGLB', 'library',
                '--RGPL', 'platform',
                '--RGPU', 'machine',
                '--RGSM', 'sample',
                '--TMP_DIR', tmp_dir]

    def run(self):
        self.output().makedirs()
        return super(AddOrReplaceReadGroups, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'grouped', self.experiment_id, self.sample_id + '.bam'))

@requires(AddOrReplaceReadGroups)
class MarkDuplicates(ExternalProgramTask):

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    # resources = {'cpu': 1, 'mem': 24}

    def program_args(self):
        return [cfg.gatk_bin,
                'MarkDuplicates',
                '--INPUT', self.input().path,
                '--OUTPUT', self.output().path,
                '--CREATE_INDEX',
                '--TMP_DIR', tmp_dir,
                '--VALIDATION_STRINGENCY', 'SILENT',
                '--METRICS_FILE', join(os.path.dirname(self.output().path), '{}.metrics'.format(self.sample_id))]

    def run(self):
        self.output().makedirs()
        return super(MarkDuplicates, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'duplicates-marked', self.experiment_id, '{}.bam'.format(self.sample_id)))

@requires(ProduceGenome, ProduceGenomeDict, MarkDuplicates)
class SplitNCigarReads(ExternalProgramTask):

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    # resources = {'cpu': 1, 'mem': 24}

    def program_args(self):
        genome, genome_dict, duplicates = self.input()
        return [cfg.gatk_bin,
                'SplitNCigarReads',
                '-R', genome.path,
                '-I', duplicates.path,
                '-O', self.output().path,
                '--tmp-dir', tmp_dir]

    def run(self):
        self.output().makedirs()
        return super(SplitNCigarReads, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'ncigar-splitted', self.experiment_id, self.sample_id + '.bam'))

@requires(ProduceGenome, SplitNCigarReads)
class CallVariants(ExternalProgramTask):

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    # resources = {'cpu': 8, 'mem': 24}

    def program_args(self):
        genome, splitted = self.input()
        return [cfg.gatk_bin,
                'HaplotypeCaller',
                "-ERC", "GVCF",
                '--native-pair-hmm-threads', "8",
                '-R', genome.path,
                '-I', splitted.path,
                '-O', self.output().path,
                '--dont-use-soft-clipped-bases',
                '--standard-min-confidence-threshold-for-calling', 30.0,
                '--tmp-dir', tmp_dir]

    def run(self):
        self.output().makedirs()
        return super(CallVariants, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'called', self.experiment_id, '{}.g.vcf.gz'.format(self.sample_id)))

@requires(ProduceGenome, CallVariants)
class FilterVariants(ExternalProgramTask):
    """Filter variants with GATK VariantFiltration tool."""

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    # resources = {'cpu': 1, 'mem': 24}

    def program_args(self):
        genome, variants = self.input()
        return [cfg.gatk_bin,
                'VariantFiltration',
                '-R', genome.path,
                '-V', variants.path,
                '-O', self.output().path,
                '--cluster-window-size', 35,
                '--cluster-size', 3,
                # '--filter-name', 'FS',
                # '--filter-expression', 'FS > 30.0',
                # '--filter-name', 'QD',
                # '--filter-expression', 'QD < 2.0',
                '--tmp-dir', tmp_dir]

    def run(self):
        self.output().makedirs()
        return super(FilterVariants, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'filtered', self.experiment_id, '{}.g.vcf.gz'.format(self.sample_id)))



@requires(FilterVariants)
class RenameVCF(luigi.Task):

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    def run(self):

        self.output().makedirs()

        name_file = join(cfg.output_dir, 'filtered', self.experiment_id, self.sample_id + ".rename")
        with open(name_file, "w") as nf:
            nf.write(self.sample_id)

        command = [
            "bcftools",
            "reheader",
            "-s", name_file,
            "-o", self.output().path,
            self.input().path
        ]

        os.system(" ".join(command))

        tabix = [
            "tabix",
            "-p", "vcf",
            self.output().path
        ]

        os.system(" ".join(tabix))

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'final', self.experiment_id, '{}.g.vcf.gz'.format(self.sample_id)))



class FilterVariantsFromExperiment(luigi.WrapperTask):
    experiment_id = luigi.Parameter()
    def requires(self):
        return [RenameVCF(self.experiment_id, os.path.basename(sample_id))
                for sample_id in glob(join(cfg.output_dir, 'data', self.experiment_id, '*'))]

#################### QUICK PIPELINE ################################################################################
@requires(ProduceGenome, SplitNCigarReads)
class CallVariantsQUICK(ExternalProgramTask):

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    # resources = {'cpu': 8, 'mem': 24}

    def program_args(self):
        genome, splitted = self.input()
        return [cfg.gatk_bin,
                'HaplotypeCaller',
                '--native-pair-hmm-threads', "8",
                '-R', genome.path,
                '-I', splitted.path,
                '-O', self.output().path,
                '--dont-use-soft-clipped-bases',
                '--standard-min-confidence-threshold-for-calling', 30.0,
                '--tmp-dir', tmp_dir]

    def run(self):
        self.output().makedirs()
        return super(CallVariantsQUICK, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'called', self.experiment_id, '{}.g.vcf.gz'.format(self.sample_id)))

@requires(ProduceGenome, CallVariantsQUICK)
class FilterVariantsQUICK(ExternalProgramTask):
    """Filter variants with GATK VariantFiltration tool."""

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    # resources = {'cpu': 1, 'mem': 24}

    def program_args(self):
        genome, variants = self.input()
        return [cfg.gatk_bin,
                'VariantFiltration',
                '-R', genome.path,
                '-V', variants.path,
                '-O', self.output().path,
                '--cluster-window-size', 35,
                '--cluster-size', 3,
                # '--filter-name', 'FS',
                # '--filter-expression', 'FS > 30.0',
                # '--filter-name', 'QD',
                # '--filter-expression', 'QD < 2.0',
                '--tmp-dir', tmp_dir]

    def run(self):
        self.output().makedirs()
        return super(FilterVariantsQUICK, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'filtered', self.experiment_id, '{}.g.vcf.gz'.format(self.sample_id)))

@requires(FilterVariantsQUICK)
class RenameVCFQUICK(luigi.Task):

    experiment_id = luigi.Parameter()
    sample_id = luigi.Parameter()

    def run(self):

        self.output().makedirs()

        name_file = join(cfg.output_dir, 'filtered', self.experiment_id, self.sample_id + ".rename")
        with open(name_file, "w") as nf:
            nf.write(self.sample_id)

        command = [
            "bcftools",
            "reheader",
            "-s", name_file,
            "-o", self.output().path,
            self.input().path
        ]

        os.system(" ".join(command))

        tabix = [
            "tabix",
            "-p", "vcf",
            self.output().path
        ]

        os.system(" ".join(tabix))

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'final', self.experiment_id, '{}.vcf.gz'.format(self.sample_id)))

class FilterVariantsFromExperimentQUICK(luigi.WrapperTask):
    experiment_id = luigi.Parameter()
    def requires(self):
        return [RenameVCF(self.experiment_id, os.path.basename(sample_id))
                for sample_id in glob(join(cfg.output_dir, 'data', self.experiment_id, '*'))]



# class CombineGVCFs(ExternalProgramTask):
#     experiment_id = luigi.Parameter()
#     def requires(self):

#         return [RenameVCF(self.experiment_id, os.path.basename(sample_id))
#                 for sample_id in glob(join(cfg.output_dir, 'data', self.experiment_id, '*'))]

#     def program_args(self):

#         root = [cfg.gatk_bin,
#                 'CombineGVCFs',
#                 '-R', cfg.genome,
#                 '-O', self.output().path,
#                 ]
#         for inp in self.input():
#             root += ["--variant", inp.path]

#         return root

#     def run(self):
#         self.output().makedirs()
#         return super(CombineGVCFs, self).run()

#     def output(self):
#         return luigi.LocalTarget(join(cfg.output_dir, 'joint_calling', self.experiment_id, "combined.g.vcf.gz"))

#################### FULL PIPELINE ################################################################################

class CombineGVCFs(ExternalProgramTask):
    experiment_id = luigi.Parameter()
    def requires(self):

        return [RenameVCF(self.experiment_id, os.path.basename(sample_id))
                for sample_id in glob(join(cfg.output_dir, 'data', self.experiment_id, '*'))]

    def program_args(self):

        root = [cfg.gatk_bin,
                # "--java-options", '"-Xmx24g"',
                'GenomicsDBImport',
                "--genomicsdb-workspace-path", os.path.dirname(self.output().path),
                "--overwrite-existing-genomicsdb-workspace",
                '--tmp-dir', tmp_dir,
                "-L",cfg.intervals
                ]
        for inp in self.input():
            root += ["-V", inp.path]

        return root

    def run(self):
        self.output().makedirs()
        os.system("rm -R "+os.path.dirname(self.output().path))
        return super(CombineGVCFs, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'joint_calling', self.experiment_id, "database", "vidmap.json"))


@requires(CombineGVCFs)
class JointCall(ExternalProgramTask):
    experiment_id = luigi.Parameter()
    def program_args(self):

        return [cfg.gatk_bin,
                'GenotypeGVCFs',
                '-R', cfg.genome,
                '-V', "gendb://"+os.path.dirname(self.input().path),
                '-O', self.output().path]

    def run(self):
        self.output().makedirs()
        return super(JointCall, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'joint_calling', self.experiment_id, "joint.vcf.gz"))

@requires(ProduceGenome, JointCall)
class FinalFilterVariants(ExternalProgramTask):
    """Filter variants with GATK VariantFiltration tool."""

    experiment_id = luigi.Parameter()

    # resources = {'cpu': 1, 'mem': 24}

    def program_args(self):
        genome, variants = self.input()
        return [cfg.gatk_bin,
                'VariantFiltration',
                '-R', genome.path,
                '-V', variants.path,
                '-O', self.output().path,
                '--cluster-window-size', 35,
                '--cluster-size', 3,
                '--filter-name', 'FS',
                '--filter-expression', 'FS > 60.0',
                '--filter-name', 'MQ',
                '--filter-expression', 'MQ < 40.0',
                '--filter-name', 'MQrs',
                '--filter-expression', 'MQRankSum < -12.5',
                '--filter-name', 'RPRS',
                '--filter-expression', 'ReadPosRankSum < -8.0',
                '--filter-name', 'QD',
                '--filter-expression', 'QD < 2.0',
                '--tmp-dir', tmp_dir]

    def run(self):
        self.output().makedirs()
        return super(FinalFilterVariants, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'joint_calling', self.experiment_id, 'joint.filtered.vcf.gz'))

@requires(ProduceGenome, FinalFilterVariants)
class RemoveFilteredVariants(ExternalProgramTask):
    """Remove marked variants with GATK VariantFiltration tool."""

    experiment_id = luigi.Parameter()

    # resources = {'cpu': 1, 'mem': 24}
    def program_args(self):
        genome, variants = self.input()
        return [cfg.gatk_bin,
                'SelectVariants',
                '-R', genome.path,
                '-V', variants.path,
                '-O', self.output().path,
                "--select-type-to-include", "SNP",
                "--exclude-filtered",
                "--restrict-alleles-to", "BIALLELIC",
                '--tmp-dir', tmp_dir]

    def run(self):
        self.output().makedirs()
        return super(RemoveFilteredVariants, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'joint_calling', self.experiment_id, 'joint.removed.final.vcf.gz'))


@requires(RemoveFilteredVariants)
class ReplaceSampleIDs(luigi.Task):
    '''
    Replaces ambiguous SNPs with integer IDs
    '''

    experiment_id = luigi.Parameter()

    def run(self):
        with gzip.open(self.input().path, "rt") as i:
            with open(self.output().path, "w") as o:
                count = 0
                for line in i:
                    if line.startswith("#"):
                        o.write(line)
                        continue
                    p_line = line.split("\t")
                    if p_line[2] != ".":
                        continue
                    else:
                        new_line = p_line[:]
                        new_line[2] = str(count)
                        count += 1
                        o.write("\t".join(new_line))



    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'joint_calling', self.experiment_id, "final.replaced.vcf"))


# @requires(ReplaceSampleIDs)
# class MAF_Filter(ExternalProgramTask):
#     '''
#     Filter for MAF
#     '''

#     experiment_id = luigi.Parameter()

#     def program_args(self):
#         command = ["plink",
#             "--vcf",
#             self.input().path,
#             "--double-id",
#             "--allow-extra-chr",
#             "--vcf-require-gt",
#             "--make-bed",
#             "--maf", "0.05",
#             "--out", self.output()[0].path.replace(".bed",'')
#         ]

#         runscript = join(cfg.output_dir, 'merged_vcf', self.experiment_id, "maf_filter.sh")

#         with open(runscript, "w") as r:
#             r.write(" ".join(command))

#         return [
#             "bash",
#             runscript
#         ]

#     def run(self):
#         self.output()[0].makedirs()
#         return super(MAF_Filter, self).run()

#     def output(self):
#         return [luigi.LocalTarget(join(cfg.output_dir, 'merged_vcf', self.experiment_id, "joint_maf-filter.bed")),
#         luigi.LocalTarget(join(cfg.output_dir, 'merged_vcf', self.experiment_id, "joint_maf-filter.fam")),
#         luigi.LocalTarget(join(cfg.output_dir, 'merged_vcf', self.experiment_id, "joint_maf-filter.bim"))]

@requires(ReplaceSampleIDs)
class ProduceHeatmap(ExternalProgramTask):
    '''
    Produce sample heatmap
    '''

    experiment_id = luigi.Parameter()

    def program_args(self):

        script = '''
library(dplyr)
library(reshape2)
library(gplots)
library(RColorBrewer)
my_palette <- brewer.pal(name="YlOrRd",n=9)

## SNPrelate
library(SNPRelate)
# these are the plink file outputs
vcf.fn <- "VCF_FILE"

# convert
#snpgdsBED2GDS(bed.fn, fam.fn, bim.fn, "GDS_FILE")
snpgdsVCF2GDS(vcf.fn, "GDS_FILE")

# open
gds <- snpgdsOpen("GDS_FILE", FALSE, TRUE, TRUE)
ibs <- snpgdsIBS(gds)
rownames(ibs$ibs) <- ibs$sample.id
colnames(ibs$ibs) <- ibs$sample.id
ibs_mat <- ibs$ibs
pdf("PDF_FILE", width = 25, height = 20)
heat_i <- heatmap.2(ibs_mat,
density.info = "none",
trace = "none",
main = paste0("EXPERIMENT"),
margins=c(10, 15))
dev.off()
snpgdsClose(gds)
'''
        script = script.replace("VCF_FILE", self.input().path)
        script = script.replace("EXPERIMENT", self.experiment_id)
        script = script.replace("GDS_FILE", join(cfg.output_dir, 'sample_heatmap', self.experiment_id, "heatmap.gds"))
        script = script.replace("PDF_FILE", self.output().path)
        runscript = join(cfg.output_dir, 'sample_heatmap', self.experiment_id, "create_heatmap.R")

        with open(runscript, "w") as r:
            r.write(script)

        return [
            "Rscript",
            runscript
        ]

    def run(self):
        self.output().makedirs()
        return super(ProduceHeatmap, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'sample_heatmap', self.experiment_id, "heatmap.pdf"))

# QUICK IDing **************************************************************************************************************************

class MergeVCFs(ExternalProgramTask):
    '''
    Merge set of filtered vcfs together
    '''

    experiment_id = luigi.Parameter()

    def program_args(self):
        inp_dir = join(cfg.output_dir, 'final', self.experiment_id) + "/*.gz"
        command = ["bcftools",
            "merge",
            inp_dir,
            "-O", "z",
            "-o", self.output().path
        ]
        runscript = join(cfg.output_dir, 'merged_vcf', self.experiment_id, "merge.sh")
        with open(runscript, "w") as r:
            r.write(" ".join(command))

        return [
            "bash",
            runscript
        ]


    def run(self):
        self.output().makedirs()
        return super(MergeVCFs, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'merged_vcf', self.experiment_id, "merged.vcf.gz"))

@requires(MergeVCFs)
class NormVCF(ExternalProgramTask):
    '''
    Normalize VCF
    '''

    experiment_id = luigi.Parameter()

    def program_args(self):
        command = ["bcftools",
            "norm",
            "-d", "snps",
            self.input().path,
            "-Ov",
            "-o", self.output().path
        ]

        runscript = join(cfg.output_dir, 'merged_vcf', self.experiment_id, "norm.sh")

        with open(runscript, "w") as r:
            r.write(" ".join(command))

        return [
            "bash",
            runscript
        ]

    def run(self):
        self.output().makedirs()
        return super(NormVCF, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'merged_vcf', self.experiment_id, "merged_norm.vcf.gz"))

@requires(NormVCF)
class ReplaceSampleIDsQUICK(luigi.Task):
    '''
    Replaces ambiguous SNPs with integer IDs
    '''

    experiment_id = luigi.Parameter()

    def run(self):
        self.output().makedirs()
        with open(self.input().path, "r") as i:
            with open(self.output().path, "w") as o:
                count = 0
                for line in i:
                    if line.startswith("#"):
                        o.write(line)
                        continue
                    p_line = line.split("\t")
                    if p_line[2] != ".":
                        continue
                    else:
                        new_line = p_line[:]
                        new_line[2] = str(count)
                        count += 1
                        o.write("\t".join(new_line))



    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'merged_vcf', self.experiment_id, "merged_norm.vcf.gz.replaced"))

@requires(ReplaceSampleIDsQUICK)
class ProduceHeatmapQUICK(ExternalProgramTask):
    '''
    Produce sample heatmap
    '''

    experiment_id = luigi.Parameter()

    def program_args(self):

        script = '''
library(dplyr)
library(reshape2)
library(gplots)
library(RColorBrewer)
my_palette <- brewer.pal(name="YlOrRd",n=9)

## SNPrelate
library(SNPRelate)
# these are the plink file outputs
vcf.fn <- "VCF_FILE"

# convert
#snpgdsBED2GDS(bed.fn, fam.fn, bim.fn, "GDS_FILE")
snpgdsVCF2GDS(vcf.fn, "GDS_FILE")

# open
gds <- snpgdsOpen("GDS_FILE", FALSE, TRUE, TRUE)
ibs <- snpgdsIBS(gds)
rownames(ibs$ibs) <- ibs$sample.id
colnames(ibs$ibs) <- ibs$sample.id
ibs_mat <- ibs$ibs
pdf("PDF_FILE", width = 25, height = 20)
heat_i <- heatmap.2(ibs_mat,
density.info = "none",
trace = "none",
main = paste0("EXPERIMENT"),
margins=c(10, 15))
dev.off()
snpgdsClose(gds)
'''
        script = script.replace("VCF_FILE", self.input().path)
        script = script.replace("EXPERIMENT", self.experiment_id)
        script = script.replace("GDS_FILE", join(cfg.output_dir, 'sample_heatmap', self.experiment_id, "heatmap.gds"))
        script = script.replace("PDF_FILE", self.output().path)
        runscript = join(cfg.output_dir, 'sample_heatmap', self.experiment_id, "create_heatmap.R")

        with open(runscript, "w") as r:
            r.write(script)

        return [
            "Rscript",
            runscript
        ]

    def run(self):
        self.output().makedirs()
        return super(ProduceHeatmapQUICK, self).run()

    def output(self):
        return luigi.LocalTarget(join(cfg.output_dir, 'sample_heatmap', self.experiment_id, "heatmap.pdf"))
