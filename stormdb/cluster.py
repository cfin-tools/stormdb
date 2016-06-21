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
# import warnings
# import numpy as np
import subprocess as subp
import re
from six import string_types
from .access import Query


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
    def __init__(self, name='hyades'):
        self.name = name

    @property
    def nodes(self):
        queue_list = self.get_load()
        return([q['name'] for q in queue_list])

    def get_load(self):
        '''Return list of queue load dictionaries'''

        output = subp.check_output(['qstat -g c'],
                                   stderr=subp.STDOUT, shell=True)

        queues = output.split('\n')[2:-1]  # throw away header lines and \n
        q_list = []
        for q in queues:
            qq = q.split()
            q_list += [dict(name=qq[0], load=qq[1], used=qq[2], avail=qq[4],
                            total=qq[5])]
        return(q_list)


class ClusterJob(object):
    ''''''
    def __init__(self, cmd=None, proj_name=None, queue='short.q'):
        self.cluster = Cluster()

        if not proj_name:
            raise(ValueError('Jobs are associated with a specific project.'))
        Query(proj_name)._check_proj_name()  # let fail if bad proj_name
        self.proj_name = proj_name

        if queue not in self.cluster.nodes:
            raise ValueError('Unknown queue ({0})!'.format(queue))
        self.queue = queue

        self._qsub_schema = QSUB_SCHEMA
        self.qsub_script = None
        self.cmd = cmd
        self._jobid = None
        self._running = False
        self._completed = False
        self._status_msg = 'Job not submitted yet'

    def _create_qsub_script(self, job_name, cwd_flag, opt_threaded_flag):
        """All variables should be defined"""
        if (self.cmd is None or self.queue is None or job_name is None or
                cwd_flag is None or opt_threaded_flag is None):
            raise ValueError('This should not happen, please report an Issue!')

        self.qsub_script =\
            self._qsub_schema.format(opt_threaded_flag=opt_threaded_flag,
                                     cwd_flag=cwd_flag, queue=self.queue,
                                     exec_cmd=self.cmd, job_name=job_name)

    def _write_qsub_job(self, sh_file='submit_job.sh'):
        """Write temp .sh"""
        with open(sh_file, 'w') as bash_file:
            bash_file.writelines(self.qsub_script)

    @staticmethod
    def _delete_qsub_job(sh_file='submit_job.sh'):
        """Delete temp .sh"""
        os.unlink(sh_file)

    def submit(self, n_threads=1, cwd=True, job_name=None, cleanup=True,
               resubmit=False, fake=False):

        if not isinstance(self.cmd, string_types):
            raise RuntimeError('Command should be a single string.')

        self._check_status()
        if self._jobid and not self._completed:
            print('Job {0} was already submitted!'.format(self._jobid))
            return
        if self._running:
            print('Job {0} is already running!'.format(self._jobid))
            return
        if self._completed and not resubmit:
            print('Job {0} is already completed, set resubmit=True to '
                  're-run.'.format(self._jobid))
            return

        opt_threaded_flag = ""
        cwd_flag = ''
        if n_threads > 1:
            opt_threaded_flag = "#$ -pe threaded {:d}".format(n_threads)
            if not self.queue == 'isis.q':
                raise ValueError('Make sure you use a parallel queue when '
                                 'submitting jobs with multiple threads.')
        if job_name is None:
            job_name = 'py-wrapper'
        if cwd:
            cwd_flag = '#$ -cwd'

        self._create_qsub_script(job_name, cwd_flag,
                                 opt_threaded_flag)
        if fake:
            print('Following script would be submitted (if not fake)')
            print(self.qsub_script)
            return

        self._write_qsub_job()
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
            self._jobid = m.group(1)
            if cleanup:
                self._delete_qsub_job()
            print('Cluster job submitted, job ID: {0}'.format(self._jobid))

    @property
    def status(self):
        self._check_status()
        return(self._status_msg)

    def _check_status(self):
        output = subp.check_output(['qstat -u ' + os.environ['USER'] +
                                    ' | grep {0}'.format(self._jobid) +
                                    ' | awk \'{print $5, $8}\''],
                                   stderr=subp.STDOUT, shell=True)

        output = output.rstrip()
        if len(output) == 0:
            if self._running and not self._completed:
                self._status_msg = 'Job completed'
                self._running = False
                self._completed = True
        else:
            runcode, hostname = output.split(' ')

            if runcode == 'r':
                queuename, exechost = hostname.split('@')
                exechost = exechost.split('.')[0]
                self._running = True
                self._completed = False
                self._status_msg = 'Running on {0} ({1})'.format(exechost,
                                                                 queuename)
            elif runcode == 'qw':
                self._running = False
                self._completed = False
                self._status_msg = 'Waiting in the queue'

    def kill(self):
        self._check_status()
        if self._running:
            try:
                subp.check_output(['qdel {0}'.format(self._jobid)],
                                  stderr=subp.STDOUT, shell=True)
            except subp.CalledProcessError:
                raise RuntimeError('This should not happen, report Issue!')
            else:
                print('Job {:s} killed.'.format(self._jobid))
                self._jobid = None
                self._running = False
                self._completed = False
                self._status_msg = 'Job was previously killed.'


class ClusterBatch(object):
    """Many ClusterJob's together
    """
    def __init__(self, proj_name, verbose=False):
        self.cluster = Cluster()
        Query(proj_name)._check_proj_name()  # let fail if bad proj_name
        self.proj_name = proj_name
        self._joblist = []

        self.logger = logging.getLogger('__name__')
        self.logger.propagate = False
        stdout_stream = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(stdout_stream)
        if verbose:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.ERROR)

    def kill(self):
        for job in self._joblist:
            job.kill()

    def build_cmd(self):
        raise RuntimeError('This should be overriden in subclasses!')

    @property
    def commands(self):
        cmdlist = [job.cmd for job in self._joblist]
        return cmdlist

    def add_job(self, cmd, queue='short.q'):
        self._joblist += [ClusterJob(cmd, self.proj_name, queue=queue)]

    @property
    def status(self, verbose=False):
        for job in self._joblist:
            job._check_status()
            print('{0}: {1}'.format(job._jobid, job.status))
            if verbose:
                print('\t{0}'.format(job.cmd))

    def submit(self, **kwargs):
        for job in self._joblist:
            if type(job) is ClusterJob:
                job.submit(**kwargs)
            else:
                raise ValueError('This should never happen, report an Issue!')
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
