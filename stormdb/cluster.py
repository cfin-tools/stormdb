"""
=========================
Methods to process data in StormDB layout on Hyades cluster
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)
import os
import sys
import logging
import warnings
import numpy as np
import subprocess as subp
from .access import DBError


class Cluster(object):
    def __init__(self):
        self.QSUB_SCHEMA = '''#HEADER\n{exec_cmd:s}'''
        self.info = dict(some='general params',
                         default_scrath='/projects/{proj_name:s}/scratch',
                         default_maxfilter='/projects/{proj_name:s}/scratch/maxfilter')
    def format_schema(self, exec_cmd=None):
        return self.QSUB_SCHEMA.format(exec_cmd=exec_cmd)

class ClusterJob(Cluster):
    def __init__(self, proj_name=None):
        super(ClusterJob, self).__init__()
        if not proj_name:
            raise(ValueError('Jobs associated with specific project'))
        self.proj_name = proj_name
        self.cmdlist = []
    def submit(self, queue='short.q'):
        for cmd in self.cmdlist:
            print('submit this\n{:s}'.format(self.format_schema(cmd)))
    def status(self):
        print('In queue, running, queue status (busy?), ...)')
    def kill(self, jobno):
        print('qdel {:s})'.format(jobno))
    def build_cmd(self):
        raise(ValueError('Override in children'))


class Maxfilter(ClusterJob):
    def __init__(self, proj_name):
        super(Maxfilter, self).__init__(proj_name)
    def build_cmd(self, infile, outfile):
        self.cmdlist += ['maxfilter {:s} {:s}'.format(infile, outfile)]

class Freesurfer(ClusterJob):
    def __init__(self, proj_name):
        super(Freesurfer, self).__init__(proj_name)
    def build_cmd(self, subject, series):
        self.cmdlist += ["recon-all -all -subjid %s -i %s" % (
                    subject, series[0]["path"] + "/" + series[0]["files"][0])]
