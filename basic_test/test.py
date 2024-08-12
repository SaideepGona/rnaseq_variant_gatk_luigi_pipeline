from glob import glob
import os
from os.path import join

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

    output_dir = luigi.Parameter()
    tmp_dir = luigi.Parameter()

cfg = rnaseq_variant_pipeline()

class MyTask(luigi.Task):
    x = luigi.IntParameter()

    def run(self):
        print(self.x)
        os.system("touch " + cfg.output_dir + "/examp")