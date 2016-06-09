"""
=========================
Methods to process data in StormDB layout, including submission to cluster.

Credits:
    Several functions are modified versions from those in mne-python
    https://github.com/mne-tools/mne-python
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

from mne.io import Raw
from mne.bem import fit_sphere_to_headshape

from .access import DBError, Query

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


def _format_qsub_schema(exec_cmd, queue, job_name, cwd_flag,
                        opt_threaded_flag):
    """All variables should be defined"""
    if (exec_cmd is None or queue is None or job_name is None or
            cwd_flag is None or opt_threaded_flag is None):
        raise ValueError('This should not happen! Contact cjb@cfin.au.dk')

    return QSUB_SCHEMA.format(opt_threaded_flag=opt_threaded_flag,
                              cwd_flag=cwd_flag,
                              queue=queue,
                              exec_cmd=exec_cmd,
                              job_name=job_name)


def _write_qsub_job(qsub_script, sh_file='submit_job.sh'):
    """Write temp .sh"""
    with open(sh_file, 'w') as bash_file:
        bash_file.writelines(qsub_script)


def _delete_qsub_job(sh_file='submit_job.sh'):
    """Delete temp .sh"""
    os.unlink(sh_file)


def submit_to_cluster(exec_cmd,
                      n_jobs=1,
                      queue='short.q',
                      cwd=True,
                      job_name=None,
                      cleanup=True):

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

    qsub_script = _format_qsub_schema(exec_cmd, queue, job_name, cwd_flag,
                                      opt_threaded_flag)

    _write_qsub_job(qsub_script)
    try:
        output = subp.check_output(['qsub', 'submit_job.sh'],
                                   stderr=subp.STDOUT,
                                   shell=False)
        # subp.check_output(['ls', 'nonexistentfile.sh'], stderr=subp.STDOUT,
        #                   shell=False)
    except subp.CalledProcessError as cpe:
        raise RuntimeError('qsub submission failed with error code '
                           '{:d}, output is:\n\n{:s}'.format(cpe.returncode,
                                                             cpe.output))
    else:
        print(output.rstrip())
        if cleanup:
            _delete_qsub_job()


class Maxfilter():
    """ Object for maxfiltering data from database into StormDB filesystem

    Parameters
    ----------
    proj_code : str
        The name of the project.
    bad : list
        List of a priori bad channels (default: empty list)
    verbose : bool
        If True (default), print out a bunch of information as we go.

    Attributes
    ----------
    proj_code : str
        Name of project
    cmd : list of str
        If defined, represents a sequence of maxfilter shell calls.
    """

    def __init__(self, proj_code, bad=[], verbose=True):
        if not os.path.exists('/projects/' + proj_code):
            raise DBError('No such project!')

        self.info = dict(proj_code=proj_code, bad=bad, cmd=[], io_mapping=[])
        # Consider placing other vars here

        self.logger = logging.getLogger('__name__')
        self.logger.propagate = False
        stdout_stream = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(stdout_stream)
        if verbose:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.ERROR)

    def detect_bad_chans_xscan(self,
                               in_fname,
                               use_tsss=False,
                               n_jobs=1,
                               xscan_bin=None,
                               set_bad=True):
        """Experimental method from Elekta for detecting bad channels

        WARNING! Use at own risk, not documented/fully tested!

        Parameters
        ----------
        in_fname : str
            Input file name
        use_tsss : bool
            If True, uses tSSS-based bad channel estimation (slow!). Default
            is False: use tSSS for particularly bad artefacts like dentals.
        xscan_bin : str
            Full path to xscan-binary (if None, default in /neuro/bin is used)
        set_bad : bool
            Set the channels found by xscan as bad in the Maxfilter object
            (default: True). NB: bad-list is amended, not replaced!
        """
        _check_n_jobs(n_jobs)

        if xscan_bin is None:
            xscan_bin = '/neuro/bin/util/xscan'

        # Start building command
        cmd = [xscan_bin, '-v', '-f', '{:s}'.format(in_fname)]

        proc = subp.Popen(cmd, shell=True, stdout=subp.PIPE)
        stdout = proc.communicate()[0]  # read stdout
        retcode = proc.wait()

        if retcode != 0:
            if retcode == 127:
                raise NameError('xscan binary ' + xscan_bin + ' not found')
            else:
                errmsg = 'xscan exited with an error, output is:\n\n' + stdout
                raise RuntimeError(errmsg)

        # CHECKME!
        bads_str = []
        for il in range(2):
            row = stdout[-1 * il]
            idx = row.find('Static')
            if idx > 0 and ('flat' in row or 'bad' in row):
                idx = row.find('): ')
                bads_str += [row[idx + 3]]

        self.logger.info('xscan detected the following bad channels:\n' +
                         bads_str)
        if set_bad:
            new_bads = bads_str.split()
            uniq_bads = [b for b in new_bads if b not in self.bad]
            self.info['bad'] = uniq_bads
            self.logger.info('Maxfilter object bad channel list updated')

    def build_maxfilter_cmd(self,
                            in_fname,
                            out_fname,
                            origin='0 0 40',
                            frame='head',
                            bad=None,
                            autobad='off',
                            skip=None,
                            force=False,
                            st=False,
                            st_buflen=16.0,
                            st_corr=0.96,
                            trans=None,
                            movecomp=False,
                            headpos=False,
                            hp=None,
                            hpistep=None,
                            hpisubt=None,
                            hpicons=True,
                            linefreq=None,
                            cal=None,
                            ctc=None,
                            mx_args='',
                            maxfilter_bin='/neuro/bin/util/maxfilter',
                            logfile=None):
        """Build a NeuroMag MaxFilter command for later execution.

        See the Maxfilter manual for details on the different options!

        Things to implement
        * check that cal-file matches date in infile!
        * check that maxfilter binary is OK

        Parameters
        ----------
        in_fname : str
            Input file name
        out_fname : str
            Output file name
        maxfilter_bin : str
            Full path to the maxfilter-executable
        logfile : str
            Full path to the output logfile
        force : bool
            Overwrite existing output (default: False)
        origin : array-like or str
            Head origin in mm. If None it will be estimated from headshape
            points.
        frame : str ('device' or 'head')
            Coordinate frame for head center
        bad : str, list (or None)
            List of static bad channels. Can be a list with channel names, or a
            string with channels (with or without the preceding 'MEG')
        autobad : string ('on', 'off', 'n')
            Sets automated bad channel detection on or off
        skip : string or a list of float-tuples (or None)
            Skips raw data sequences, time intervals pairs in sec,
            e.g.: 0 30 120 150
        force : bool
            Ignore program warnings
        st : bool
            Apply the time-domain SSS extension (tSSS)
        st_buflen : float
            tSSS buffer length in sec (disabled if st is False)
        st_corr : float
            tSSS subspace correlation limit (disabled if st is False)
        movecomp : bool (or 'inter')
            Estimates and compensates head movements in continuous raw data.
        trans : str(filename or 'default') (or None)
            Transforms the data into the coil definitions of in_fname,
            or into the default frame. If None, and movecomp is True,
            data will be movement compensated to initial head position.
        headpos : bool
            Estimates and stores head position parameters, but does not
            compensate movements
        hp : string (or None)
            Stores head position data in an ascii file
        hpistep : float (or None)
            Sets head position update interval in ms
        hpisubt : str('amp', 'base', 'off') (or None)
            Subtracts hpi signals: sine amplitudes, amp + baseline, or switch
            off
        hpicons : bool
            Check initial consistency isotrak vs hpifit
        linefreq : int (50, 60) (or None)
            Sets the basic line interference frequency (50 or 60 Hz)
            (None: do not use line filter)
        cal : str
            Path to calibration file
        ctc : str
            Path to Cross-talk compensation file
        mx_args : str
            Additional command line arguments to pass to MaxFilter
        """
        # determine the head origin if necessary
        if origin is None:
            self.logger.info('Estimating head origin from headshape points..')
            raw = Raw(in_fname, preload=False)
            with warnings.filterwarnings('error', category=RuntimeWarning):
                r, o_head, o_dev = fit_sphere_to_headshape(raw.info,
                                                           dig_kind='auto',
                                                           units='m')
            raw.close()

            self.logger.info('Fitted sphere: r = {.1f} mm'.format(r))
            self.logger.info(
                'Origin head coordinates: {.1f} {.1f} {.1f} mm'.format(o_head[
                    0], o_head[1], o_head[2]))
            self.logger.info(
                'Origin device coordinates: {.1f} {.1f} {.1f} mm'.format(o_dev[
                    0], o_dev[1], o_dev[2]))

            self.logger.info('[done]')
            if frame == 'head':
                origin = o_head
            elif frame == 'device':
                origin = o_dev
            else:
                RuntimeError('invalid frame for origin')

        # Start building command
        cmd = (
            maxfilter_bin + ' -f {:s} -o {:s} -v '.format(in_fname, out_fname))

        if isinstance(origin, (np.ndarray, list, tuple)):
            origin = '{:.1f} {:.1f} {:.1f}'.format(origin[0], origin[1],
                                                   origin[2])
        elif not isinstance(origin, str):
            raise (ValueError('origin must be list-like or string'))

        cmd += ' -frame {:s} -origin {:s} -v '.format(frame, origin)

        if bad is not None:
            # format the channels
            if isinstance(bad, str):
                bad = bad.split()
            bad += self.info['bad']  # combine the two
        else:
            bad = self.info['bad']

        if len(bad) > 0:
            # now assume we have a list of str with channel names
            bad_logic = [ch[3:] if ch.startswith('MEG') else ch for ch in bad]
            bad_str = ' '.join(bad_logic)

            cmd += '-bad {:s} '.format(bad_str)

        cmd += '-autobad {:s} '.format(autobad)

        if skip is not None:
            if isinstance(skip, list):
                skip = ' '.join(['{:.3f} {:.3f}'.format(s[0], s[1])
                                 for s in skip])
            cmd += '-skip {:s} '.format(skip)

        if force:
            cmd += '-force '

        if st:
            cmd += '-st '
            cmd += ' {:.0f} '.format(st_buflen)
            cmd += '-corr {:.4f} '.format(st_corr)

        if trans is not None:
            cmd += '-trans {:s} '.format(trans)

        if movecomp:
            cmd += '-movecomp '
            if movecomp == 'inter':
                cmd += ' inter '

        if headpos:
            if movecomp:
                raise RuntimeError('movecomp and headpos mutually exclusive')
            cmd += '-headpos '

        if hp is not None:
            cmd += '-hp {:s} '.format(hp)

        if hpisubt is not None:
            cmd += 'hpisubt {:s} '.format(hpisubt)

        if hpicons:
            cmd += '-hpicons '

        if linefreq is not None:
            cmd += '-linefreq {:d} '.format(linefreq)

        if cal is not None:
            cmd += '-cal {:s} '.format(cal)

        if ctc is not None:
            cmd += '-ctc {:s} '.format(ctc)

        cmd += mx_args

        if logfile:
            cmd += ' | tee ' + logfile

        self.info['cmd'] += [cmd]
        self.info['io_mapping'] += [dict(input=in_fname, output=out_fname)]

    def submit_to_cluster(self, n_jobs=1, fake=False, submit_script=None):
        """ Submit the command built earlier for processing on the cluster.

        Things to implement
        * check output?

        Parameters
        ----------
        n_jobs : int
            Number of parallel threads to allow (Intel MKL). Max 12!
        fake : bool
            If true, run a fake run, just print the command that will be
            submitted.
        """
        if len(self.info['cmd']) < 1:
            raise NameError('cmd to submit is not defined yet')

        for ic, cmd in enumerate(self.info['cmd']):
            if not fake:
                self.logger.info('Submitting command:\n{:s}'.format(cmd))

                submit_to_cluster(cmd,
                                  n_jobs=n_jobs,
                                  queue='isis.q',
                                  job_name='maxfilter')

                self.info['cmd'] = []  # clear list for next round
                self.info['io_mapping'] = []  # clear list for next round
            else:
                print('{:d}: {:s}'.format(ic + 1, self.info['io_mapping'][ic][
                    'input']))
                print('\t-->{:s}'.format(self.info['io_mapping'][ic][
                    'output']))


class FS_reconstruction():
    """ Object for FreeSurfer recon-all data from database into StormDB filesystem

       Things to implement
       * single sbuject, with force param
    """

    def __init__(self, proj_code, verbose=True):
        if not os.path.exists('/projects/' + proj_code):
            raise DBError('No such project!')

        self.info = dict(proj_code=proj_code, cmd=[])
        # Consider placing other vars here

        self.logger = logging.getLogger('__name__')
        self.logger.propagate = False
        stdout_stream = logging.StreamHandler(sys.stdout)
        self.logger.addHandler(stdout_stream)
        if verbose:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.ERROR)

        self.fs_subjects_dir = os.path.join("/projects", proj_code,
                                            "scratch/fs_subjects_dir")
        os.environ["SUBJECTS_DIR"] = self.fs_subjects_dir

    def all_subjects(self):
        db = Query(self.info["proj_code"])

        included_subjects = db.get_subjects()

        for subject in included_subjects[:-1]:
            # this is an example of getting the DICOM files as a list
            mr_study = db.get_studies(subject, modality='MR', unique=True)
            if len(mr_study) > 0:
                # This is a 2D list with [series_name, series_number]
                series = db.filter_series(description="t1*",
                                          subj_ids=subject,
                                          modalities="MR")
                if len(series) == 1:  # TODO: make more pythonic
                    cmd = "recon-all -all -subjid %s -i %s" % (
                        subject,
                        series[0]["path"] + "/" + series[0]["files"][0])
                    self.info["cmd"] += [cmd]
                else:
                    print("\nProblen with T1 for subject: %s" % subject,
                          "Either none or multiple T1\'s "
                          "present for subject")

    def single_subject(self, subject=None):
        """Make FreeSurfer reconstruction for a sinlge subject.

        Params
        ------
        subject : str
            The subject id from the Stormdb.
        """
        if os.path.exists(self.fs_subjects_dir + subject):
            raise ValueError("No such project!Subject exist in" +
                             "fs_subjects_dir! \nPlease delete" +
                             "or rename subject.")

        db = Query(self.info["proj_code"])
        # TODO: Add test that subject name is in stormdb

        mr_study = db.get_studies(subject, modality='MR', unique=True)

        if len(mr_study) > 0:
            # This is a 2D list with [series_name, series_number]
            series = db.filter_series(description="t1*",
                                      subj_ids=subject,
                                      modalities="MR")
            if len(series) == 1:  # TODO: make more pythonic
                cmd = "recon-all -all -subjid %s -i %s" % (
                    subject, series[0]["path"] + "/" + series[0]["files"][0])
                self.info["cmd"] += [cmd]
            else:
                print("\nProblen with T1 for subject: %s" % subject,
                      "Either none or multiple T1\'s "
                      "present for subject")
        else:
            print("No MR for subject: %s" % subject)

    def submit_to_cluster(self, n_jobs=1, fake=False, submit_script=None):
        """ Submit the command built earlier for processing on the cluster.

        Things to implement
        * check output?

        Parameters
        ----------
        n_jobs : int
            Number of parallel threads to allow (Intel MKL). Max 12!
        fake : bool
            If true, run a fake run, just print the command that will be
            submitted.
        """
        if len(self.info['cmd']) < 1:
            raise NameError('cmd to submit is not defined yet')

        for cmd in self.info['cmd']:
            if not fake:
                self.logger.info('Submitting command:\n{:s}'.format(cmd))

                submit_to_cluster(cmd,
                                  n_jobs=n_jobs,
                                  queue='isis.q',
                                  job_name='FS_recon')

                self.info['cmd'] = []  # clear list for next roun
            else:
                print(self.info["cmd"])


def _check_n_jobs(n_jobs):
    """Check that n_jobs is sane"""
    if n_jobs > 12:
        raise ValueError('isis only has 12 cores!')
    elif n_jobs < 1 or type(n_jobs) is not int:
        raise ValueError('number of jobs must be a positive integer!')
