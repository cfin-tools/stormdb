"""
=========================
Classes to process data in StormDB layout on Hyades cluster
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)
import os
import sys
import logging
import subprocess as subp
import re
from six import string_types
from os.path import expanduser
from .access import Query
from .base import enforce_path_exists


QSUB_SCHEMA = """
#$ -S /bin/bash
# Pass on all environment variables
#$ -V
# Operate in current working directory
{cwd_flag:s}
#$ -N {job_name:s}
#$ -o {log_name_prefix:s}_$JOB_ID.qsub
# Merge stdout and stderr
#$ -j y
#$ -q {queue:s}
{opt_threaded_flag:s}
{opt_h_vmem_flag:s}
{opt_mem_free_flag:s}

# Make sure process uses max requested number of threads!
export OMP_NUM_THREADS=$NSLOTS

echo "Executing following command on $NSLOTS threads:"
echo -e {exec_cmd:s}

{exec_cmd:s}  # remember to escape quotes on command-liners!

echo "Done executing"
"""


class Cluster(object):
    """Class to represent the cluster itself, with diagnostic methods.

    Parameters
    ----------
    name : str
        Name of the cluster (default: hyades)

    Attributes
    ----------
    queues : list
        List of queue names defined on cluster.
    parallel_envs : list
        List of parallel environment names names defined on cluster.
    """
    def __init__(self, name='hyades'):
        self.name = name
        self._highmem_qs = ['highmem.q']

    def _query(self, cmd):
        """Return list of outputs from a shell call"""
        try:
            output = subp.check_output([cmd],
                                       stderr=subp.STDOUT, shell=True)
        except subp.CalledProcessError as cpe:
            raise RuntimeError('Command {:s} failed with error code {:d}, '
                               'output is:\n\n{:s}'.format(cmd,
                                                           cpe.returncode,
                                                           cpe.output))
        # NB the decode-step here is important: in Py3, check_output
        # returns a byte-string! This is tested to work on Py2
        output = output.decode('ascii', 'ignore')
        # first strip whitespace (incl. \n), then split on newline
        return(output.rstrip().split('\n'))

    @property
    def queues(self):
        return(self._query('qconf -sql'))

    @property
    def parallel_envs(self):
        return(self._query('qconf -spl'))

    def get_memlimit_per_process(self, queue):
        """Get value of h_vmem (memory limit/process) for specified queue."""
        if queue not in self.queues:
            raise ValueError('Unknown queue: {:s}'.format(queue))

        lim = self._query('qconf -sq ' + queue +
                          '| grep h_vmem | awk {\'print $2\'}')[0]

        _, lim_int, lim_units = re.split('(\d+)', lim)
        assert isinstance(int(lim_int), int)
        assert isinstance(lim_units, string_types)

        return(lim)

    def _check_parallel_env(self, queue, pe_name):
        """Check that a PE is in the pe_list for a given queue"""
        pes = self._query('qconf -sq ' + queue +
                          '| grep pe_list')[0]  # just one line
        pe_list = pes.split()[1:]
        if pe_name not in pe_list:
            raise ValueError('Queue \'{0}\' does not support the \'{1}\' '
                             'parallel environment.'.format(queue, pe_name))

    def get_load_dict(self):
        '''Return list of queue load dictionaries'''
        # throw away header lines and \n
        loads = self._query('qstat -g c')[2:-1]
        q_list = []
        for q in loads:
            qq = q.split()
            q_list += [dict(name=qq[0], load=qq[1], used=qq[2], avail=qq[4],
                            total=qq[5])]
        return(q_list)


class ClusterJob(object):
    """Class to represent a single job on the cluster.

    Parameters
    ----------
    cmd : str | list of str
        The shell command(s) to submit to the cluster as a single job.
    proj_name : str | None
        The name of the project. If None, will read MINDLABPROJ from
        environment.
    queue : str
        The name of the queue to submit the job to (default: 'short.q').
    h_vmem : str | None
        Specify the limit on the amount of combined memory consumed by all
        the processes in the job. This parameter only has an effect for queues
        that support setting the parameter. The format is in the style "50G".
    n_threads : int
        If > 1 (default), the job must be submitted to a queue that is capable
        of multi-threaded parallelism.
    working_dir : str
        Set the job's working directory. May either be an existing path, or
        'cwd' for current working directory (default: 'cwd').
    job_name : str | None
        Name of job (shows up in the output of qstat). If None, "py-wrapper"
        is used.
    cleanup : bool
        Delete qsub bash-script after submission (default: True)

    Attributes
    ----------
    cluster : instance of Cluster
        Cluster object for status checking etc.
    proj_name : str
        The StormDB project name.
    queue : str
        The name of the queue the job will be submitted to.
    n_threads : int
        Number of threads to run on.
    cmd : str
        The command (if several, separated by ';') to be submitted (cannot
        be modified once defined).
    """
    def __init__(self, cmd=None, proj_name=None, queue='short.q', h_vmem=None,
                 mem_free=None, n_threads=1, working_dir='cwd', job_name=None,
                 log_dir=None, cleanup=True):
        self.cluster = Cluster()

        if not cmd:
            raise(ValueError('You must specify the command to run!'))
        if not proj_name:
            raise(ValueError('Jobs are associated with a specific project.'))
        Query(proj_name)._check_login_credentials()
        self.proj_name = proj_name

        if queue not in self.cluster.queues:
            raise ValueError('Unknown queue ({0})!'.format(queue))
        if queue in self.cluster._highmem_qs and h_vmem is None:
            raise RuntimeError('You must specify the anticipated memory '
                               'usage for the {:s} queue using the option: '
                               'h_vmem'.format(queue))

        self.queue = queue
        self.n_threads = n_threads
        self.h_vmem = h_vmem
        self.mem_free = mem_free
        self.log_dir = log_dir

        self._qsub_schema = QSUB_SCHEMA
        self._qsub_script = None
        self._initialise_cmd(cmd)  # let the initialiser do the checking
        self._jobid = None
        self._running = False
        self._waiting = False
        self._completed = False
        self._submitted = False
        self._status_msg = 'Job not submitted yet'
        self._cleanup_qsub_job = cleanup

        opt_threaded_flag = ""
        opt_h_vmem_flag = ""
        opt_mem_free_flag = ""
        cwd_flag = ''
        if self.n_threads > 1:
            self.cluster._check_parallel_env(self.queue, 'threaded')
            opt_threaded_flag = "#$ -pe threaded {:d}".format(self.n_threads)
        if self.h_vmem is not None:
            # XXX would be nice with some sanity checking here...
            opt_h_vmem_flag = "#$ -l h_vmem={:s}".format(self.h_vmem)
        if self.mem_free is not None:
            # XXX would be nice with some sanity checking here...
            opt_mem_free_flag = "#$ -l mem_free={:s}".format(self.mem_free)
        if job_name is None:
            job_name = 'py-wrapper'
        log_name_prefix = job_name

        if working_dir is not None and isinstance(working_dir, string_types):
            if working_dir == 'cwd':
                cwd_flag = '#$ -cwd'
            else:
                enforce_path_exists(working_dir)
                cwd_flag = '#$ -wd {:s}'.format(working_dir)
        if self.log_dir is not None:
            if not os.path.exists(self.log_dir):
                raise ValueError(
                    'Log directory {} does not exist.'.format(self.log_dir))
            log_name_prefix = os.path.join(self.log_dir, job_name)

        self._create_qsub_script(job_name, cwd_flag,
                                 opt_threaded_flag, opt_h_vmem_flag,
                                 opt_mem_free_flag, log_name_prefix)

    @property
    def cmd(self):
        return self._cmd

    @cmd.setter
    def cmd(self, value):
        raise ValueError('Once the command is set, it cannot be changed!')

    def _initialise_cmd(self, value):
        if isinstance(value, list):
            if not all(isinstance(s, string_types) for s in value):
                raise RuntimeError('Each element of the command list should '
                                   'be a single string.')
            else:
                self._cmd = '\n'.join(value)
        elif not isinstance(value, string_types):
            raise RuntimeError('Command should be a single string.')
        else:
            self._cmd = value

    def _create_qsub_script(self, job_name, cwd_flag, opt_threaded_flag,
                            opt_h_vmem_flag, opt_mem_free_flag,
                            log_name_prefix):
        """All variables should be defined"""
        if (self.cmd is None or self.queue is None or job_name is None or
                cwd_flag is None or opt_threaded_flag is None or
                opt_h_vmem_flag is None or opt_mem_free_flag is None):
            raise ValueError('This should not happen, please report an Issue!')

        self._qsub_script =\
            self._qsub_schema.format(opt_threaded_flag=opt_threaded_flag,
                                     opt_h_vmem_flag=opt_h_vmem_flag,
                                     opt_mem_free_flag=opt_mem_free_flag,
                                     cwd_flag=cwd_flag, queue=self.queue,
                                     log_name_prefix=log_name_prefix,
                                     exec_cmd=self.cmd, job_name=job_name)

    def _write_qsub_job(self, sh_file='~/submit_job.sh'):
        """Write temp .sh"""
        with open(expanduser(sh_file), 'w') as bash_file:
            bash_file.writelines(self._qsub_script)

    @staticmethod
    def _delete_qsub_job(sh_file='~/submit_job.sh'):
        """Delete temp .sh"""
        os.unlink(expanduser(sh_file))

    def submit(self, fake=False, sh_file='~/submit_job.sh'):

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
            print('Following command would be submitted (if not fake)')
            print(self._cmd)
            return

        self._write_qsub_job()
        try:
            output = subp.check_output(['qsub', expanduser(sh_file)],
                                       stderr=subp.STDOUT, shell=False)
        except subp.CalledProcessError as cpe:
            raise RuntimeError('qsub submission failed with error code {:d}, '
                               'output is:\n\n{:s}'.format(cpe.returncode,
                                                           cpe.output))
        else:
            # py2-3 safety
            output = output.decode('ascii', 'ignore').rstrip()
            m = re.search('(\d+)', output)
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
        output = self.cluster._query('qstat -u ' + os.environ['USER'] +
                                     ' | grep {0}'.format(self._jobid) +
                                     ' | awk \'{print $5, $8}\'')[0]  # ONLY

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
    """Many ClusterJob's to be submitted together as a batch.

    This docstring should be overwritten by the children.
    """
    def __init__(self, proj_name, verbose=False):
        self.cluster = Cluster()
        # let fail if bad proj_name
        qy = Query(proj_name)  # if None, read proj_name from env
        qy._check_login_credentials()
        self.proj_name = qy.proj_name
        self._joblist = []

        self.logger = logging.getLogger('ClusterBatchLogger')
        # Only create a new handler if none exist
        if len(self.logger.handlers) == 0:
            self.logger.propagate = False
            stdout_stream = logging.StreamHandler(sys.stdout)
            self.logger.addHandler(stdout_stream)
        self.verbose = verbose

        # Get docstring for add_job from ClusterJob.__init__!
        doc = ClusterJob.__doc__
        doc = doc[doc.find('\n'):]  # Strip first line
        doc = "Add a ClusterJob to the list (batch) of jobs." + doc
        self.add_job.__func__.__doc__ = doc

    @property
    def verbose(self):
        if self.logger.level > logging.DEBUG:
            return False
        else:
            return True

    @verbose.setter
    def verbose(self, value):
        """Set to True for more detailed runtime information."""
        if not isinstance(value, bool):
            raise RuntimeError('Set verbose to True or False!')
        elif value is True:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)

    def kill(self, jobid=None):
        """Kill (delete) all the jobs in the batch."""
        for job in self._joblist:
            if (jobid is None or
                    (jobid is not None and int(job._jobid) == int(jobid))):
                job.kill()

    def build_cmd(self):
        raise RuntimeError('This should be overriden in subclasses!')

    @property
    def commands(self):
        """Return list of commands in the batch."""
        cmdlist = [job.cmd for job in self._joblist]
        return cmdlist

    def add_job(self, cmd, **kwargs):
        """This is replaced in __init__ by ClusterJob.__doc__!
        """
        self._joblist += [ClusterJob(cmd, self.proj_name, **kwargs)]

    @property
    def status(self):
        """Print status of cluster jobs."""
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
