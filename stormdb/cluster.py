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
#$ -S /bin/bash
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

echo {exec_cmd:s}
#eval "{exec_cmd:s}"
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
    def __init__(self, cmd=None, proj_name=None, queue='short.q', n_threads=1,
                 cwd=True, job_name=None, cleanup=True):
        self.cluster = Cluster()

        if not cmd:
            raise(ValueError('You must specify the command to run!'))
        if not proj_name:
            raise(ValueError('Jobs are associated with a specific project.'))
        Query(proj_name)._check_proj_name()  # let fail if bad proj_name
        self.proj_name = proj_name

        if queue not in self.cluster.nodes:
            raise ValueError('Unknown queue ({0})!'.format(queue))
        self.queue = queue

        self._qsub_schema = QSUB_SCHEMA
        self.qsub_script = None
        self._cmd = cmd
        self._jobid = None
        self._running = False
        self._waiting = False
        self._completed = False
        self._submitted = False
        self._status_msg = 'Job not submitted yet'
        self._cleanup_qsub_job = cleanup

        opt_threaded_flag = ""
        cwd_flag = ''
        if n_threads > 1:
            opt_threaded_flag = "#$ -pe threaded {:d}".format(n_threads)
            if not queue == 'isis.q':
                raise ValueError('Make sure you use a parallel queue when '
                                 'submitting jobs with multiple threads.')
        if job_name is None:
            job_name = 'py-wrapper'
        if cwd:
            cwd_flag = '#$ -cwd'

        self._create_qsub_script(job_name, cwd_flag,
                                 opt_threaded_flag)

    @property
    def cmd(self):
        return self._cmd

    @cmd.setter
    def cmd(self, value):
        if not isinstance(value, string_types):
            raise RuntimeError('Command should be a single string.')
        else:
            self._cmd = value

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

    def submit(self, fake=False):

        self._check_status()
        if self._submitted:
            if self._running:
                print('Job {0} is already running!'.format(self._jobid))
                return
            elif self._waiting:
                print('Job {0} is already waiting!'.format(self._jobid))
                return
            elif self._completed:
                print('Job {0} is already completed, re-create job to '
                      're-run.'.format(self._jobid))
                return
            else:
                print('Job {0} was already submitted.'.format(self._jobid))
                return

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
            if self._cleanup_qsub_job:
                self._delete_qsub_job()
            print('Cluster job submitted, job ID: {0}'.format(self._jobid))
            self._submitted = True

    @property
    def status(self):
        self._check_status()
        return(self._status_msg)

    def _check_status(self):
        if self._completed:
            return
        output = subp.check_output(['qstat -u ' + os.environ['USER'] +
                                    ' | grep {0}'.format(self._jobid) +
                                    ' | awk \'{print $5, $8}\''],
                                   stderr=subp.STDOUT, shell=True)

        output = output.rstrip()
        if len(output) == 0:
            if (self._submitted and not self._running and
                    not self._completed and not self._waiting):
                self._status_msg = ('Submission failed, see log for'
                                    ' output errors!')
            elif self._submitted and not self._completed:
                if self._running:
                    self._status_msg = 'Job completed'
                    self._running, self._waiting = False, False
                    self._completed = True
        else:
            runcode, hostname = output.split(' ')

            if runcode == 'r':
                queuename, exechost = hostname.split('@')
                exechost = exechost.split('.')[0]
                self._running = True
                self._waiting = False
                self._completed = False
                self._status_msg = 'Running on {0} ({1})'.format(exechost,
                                                                 queuename)
            elif runcode == 'qw':
                self._running = False
                self._waiting = True
                self._completed = False
                self._status_msg = 'Waiting in the queue'
            else:
                self._running = False
                self._waiting = True
                self._completed = False
                self._status_msg = ('Queue status odd (qstat says: {0}), '
                                    'please check!'.format(runcode))

    def kill(self):
        self._check_status()
        if self._submitted and (self._running or self._waiting):
            try:
                subp.check_output(['qdel {0}'.format(self._jobid)],
                                  stderr=subp.STDOUT, shell=True)
            except subp.CalledProcessError:
                raise RuntimeError('This should not happen, report Issue!')
            else:
                print('Job {:s} killed. You must manually delete any output '
                      'it may have created!'.format(self._jobid))
                self._running = False
                self._waiting = False
                self._completed = False
                self._status_msg = 'Job was previously killed.'


class ClusterBatch(object):
    """Many ClusterJob's to be submitted together as a batch
    """
    def __init__(self, proj_name, verbose=False):
        self.cluster = Cluster()
        Query(proj_name)._check_proj_name()  # let fail if bad proj_name
        self.proj_name = proj_name
        self._joblist = []

        self.logger = logging.getLogger('ClusterBatchLogger')
        # Only create a new handler if none exist
        if len(self.logger.handlers) == 0:
            self.logger.propagate = False
            stdout_stream = logging.StreamHandler(sys.stdout)
            self.logger.addHandler(stdout_stream)
        self.verbose = verbose

    @property
    def verbose(self):
        if self.logger.getLevel() > logging.DEBUG:
            return False
        else:
            return True

    @verbose.setter
    def verbose(self, value):
        if not isinstance(value, bool):
            raise RuntimeError('Set verbose to True or False!')
        elif value is True:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

    def kill(self, jobid=None):
        for job in self._joblist:
            if (jobid is None or
                    (jobid is not None and int(job._jobid) == int(jobid))):
                job.kill()

    def build_cmd(self):
        raise RuntimeError('This should be overriden in subclasses!')

    @property
    def commands(self):
        cmdlist = [job.cmd for job in self._joblist]
        return cmdlist

    def add_job(self, cmd, **kwargs):
        # queue='short.q', n_threads=1,
        #         cwd=True, job_name=None, cleanup=True):
        """See ClusterJob for arguments!
        """
        self._joblist += [ClusterJob(cmd, self.proj_name, **kwargs)]

    @property
    def status(self):
        for ij, job in enumerate(self._joblist):
            self.logger.info('#{ij:d} ({jid:}): '
                             '{jst}'.format(ij=ij + 1, jid=job._jobid,
                                            jst=job.status))
            self.logger.debug('\t{0}'.format(job.cmd))

    def submit(self, fake=False):
        """Submit a batch of jobs.

        Parameters
        ----------
        fake : bool
            If True, show what would be submitted (but don't actually submit).
        """
        for job in self._joblist:
            if type(job) is ClusterJob:
                job.submit(fake=fake)
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
