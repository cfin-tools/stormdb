"""
=========================
Methods to process data in StormDB layout on Hyades cluster
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)
import os
# import sys
# import logging
# import warnings
# import numpy as np
import subprocess as subp
import re
# from .access import DBError


QSUB_SCHEMA = """
#!/bin/bash

# Pass on all environment variables
#$ -V
# Operate in current working directory
{cwd_flag:s}
#$ -N {job_name:s}
#$ -o {job_name:s}_$JOB_ID.qsub
# Merge stdout and stderr
#$ -j y
#$ -q {queue:s}
{opt_threaded_flag:s}

# Make sure process uses max requested number of threads!
export OMP_NUM_THREADS=$NSLOTS

echo "Executing following command on $NSLOTS threads:"
echo "{exec_cmd:s}"

{exec_cmd:s}

echo "Done executing"
"""


class Cluster(object):
    def __init__(self, proj_name=None):
        pass

    def status(self):
        output = subp.check_output(['qstat -u ', os.environ['USER']],
                                   stderr=subp.STDOUT, shell=False)

        print(output)

    def kill(self, jobno):
        print('qdel {:s})'.format(jobno))


class ClusterJob(object):
    ''''''
    def __init__(self, proj_name=None):
        if not proj_name:
            raise(ValueError('Jobs associated with specific project'))
        self.proj_name = proj_name

        super(ClusterJob, self).__init__()
        self._qsub_schema = QSUB_SCHEMA
        self.cmd = None
        self.jobid = None
        self.running = False

    def _format_qsub_schema(self, exec_cmd, queue, job_name, cwd_flag,
                            opt_threaded_flag):
        """All variables should be defined"""
        if (exec_cmd is None or queue is None or job_name is None or
                cwd_flag is None or opt_threaded_flag is None):
            raise ValueError('This should not happen, please report an Issue!')

        return self._qsub_schema.format(opt_threaded_flag=opt_threaded_flag,
                                        cwd_flag=cwd_flag, queue=queue,
                                        exec_cmd=exec_cmd, job_name=job_name)

    @staticmethod
    def _write_qsub_job(qsub_script, sh_file='submit_job.sh'):
        """Write temp .sh"""
        with open(sh_file, 'w') as bash_file:
            bash_file.writelines(qsub_script)

    @staticmethod
    def _delete_qsub_job(sh_file='submit_job.sh'):
        """Delete temp .sh"""
        os.unlink(sh_file)

    def submit(self, exec_cmd, n_jobs=1, queue='short.q', cwd=True,
               job_name=None, cleanup=True):

        opt_threaded_flag = ""
        cwd_flag = ''
        if n_jobs > 1:
            opt_threaded_flag = "#$ -pe threaded {:d}".format(n_jobs)
            if not queue == 'isis.q':
                raise ValueError('Make sure you use a parallel queue when '
                                 'submitting jobs with multiple threads.')
        if job_name is None:
            job_name = 'py-wrapper'
        if cwd:
            cwd_flag = '#$ -cwd'

        qsub_script = self._format_qsub_schema(exec_cmd, queue, job_name,
                                               cwd_flag, opt_threaded_flag)

        self._write_qsub_job(qsub_script)
        try:
            output = subp.check_output(['qsub', 'submit_job.sh'],
                                       stderr=subp.STDOUT, shell=False)
        except subp.CalledProcessError as cpe:
            raise RuntimeError('qsub submission failed with error code {:d}, '
                               'output is:\n\n{:s}'.format(cpe.returncode,
                                                           cpe.output))
        else:
            # print(output.rstrip())
            m = re.search('(\d+)', output.rstrip())
            self.jobid = m.group(1)
            self.running = True
            if cleanup:
                self._delete_qsub_job()

    # override from base class to refer only to this job
    def status(self):
        output = subp.check_output(['qstat -u ', os.environ['USER'],
                                    ' | grep {:d}'.format(self.jobid),
                                    ' | awk \'{print $5, $8}\''],
                                   stderr=subp.STDOUT, shell=False)

        return(output)

    def kill(self):
        print('qdel {:s})'.format(self.jobid))


# class Maxfilter(ClusterJob):
#     def __init__(self, proj_name):
#         super(Maxfilter, self).__init__(proj_name)
#     def build_cmd(self, infile, outfile):
#         self.cmdlist += ['maxfilter {:s} {:s}'.format(infile, outfile)]
#
# class Freesurfer(ClusterJob):
#     def __init__(self, proj_name):
#         super(Freesurfer, self).__init__(proj_name)
#     def build_cmd(self, subject, series):
#         self.cmdlist += ["recon-all -all -subjid %s -i %s" % (
#                     subject, series[0]["path"] + "/" + series[0]["files"][0])]
