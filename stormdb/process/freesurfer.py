"""
=========================
Classes related to Freesurfer
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)
import os
import subprocess as subp
import shutil

from six import string_types

from .utils import first_file_in_dir, make_copy_of_dicom_dir
from ..base import (enforce_path_exists, check_source_readable,
                    _get_unique_series)
from ..access import Query
from ..cluster import ClusterBatch


class Freesurfer(ClusterBatch):
    """ Object for running Freesurfer in the StormDB environment

    Example 1: Run `recon-all -all` on all subjects in the database that
    include a study with an MR-modality present. Ensure that each subject
    has an MR-series matching the wildcard '*t1*mpr*'. Also use the default
    option '-3T' for non-uniformity correction at 3T. The jobs will be
    submitted to 'long.q' and run on 1 thread per job (defaults).
        >>> from stormdb.process import Freesurfer  # doctest: +SKIP
        >>> fs = Freesurfer(subjects_dir='scratch/fs_subjects_dir',
                            t1_series='*t1*mpr*')  # doctest: +SKIP
        >>> fs.recon_all('all')  # doctest: +SKIP
        >>> fs.submit()  # doctest: +SKIP

    Parameters
    ----------
    proj_name : str | None
        The name of the project. If None, will read MINDLABPROJ from
        environment.
    subjects_dir : str | None
        Path to the Freesurfer SUBJECTS_DIR. You may also specify the path
        relative to the project directory (e.g. 'scratch/fs_subjects_dir').
        If None, we'll try to read the corresponding environment variable
        from the shell (default).
    t1_series : str (optional)
        The name of the T1-weighted MR series to use for cortex extraction.
        This parameter is optional, it only has an effect when running
        recon-all for the first time (mri_convert from DICOM to mgz).
    verbose : bool
        If True, print out extra information as we go (default: False).

    Attributes
    ----------
    info : dict
        See `Freesurfer().info.keys()` for contents.
    """

    def __init__(self, proj_name=None, subjects_dir=None, t1_series=None,
                 verbose=False):
        super(Freesurfer, self).__init__(proj_name, verbose=verbose)

        if subjects_dir is None:
            if 'SUBJECTS_DIR' in os.environ.keys():
                subjects_dir = os.environ['SUBJECTS_DIR']
            else:
                raise ValueError('No SUBJECTS_DIR defined! You must do so '
                                 'either by using an argument to this method, '
                                 'or by setting the SUBJECT_DIR environment '
                                 'variable. The directory must exist.')
        else:
            if not subjects_dir.startswith('/'):
                # the path can be _relative_ to the project dir
                subjects_dir = os.path.join('/projects', self.proj_name,
                                            subjects_dir)

        enforce_path_exists(subjects_dir)

        valid_subjects = Query(proj_name).get_subjects(has_modality='MR')
        if len(valid_subjects) == 0:
            raise RuntimeError(
                'No subjects with MR-modality found in {}!'
                .format(self.proj_name))

        self.info = dict(valid_subjects=valid_subjects,
                         subjects_dir=subjects_dir)

        if t1_series is not None:
            self.info.update(t1_series=t1_series)

        self.verbose = verbose

        # Consider placing other vars here

    def recon_all(self, subject, t1_series=None, hemi='both',
                  directives=['all', '3T'], analysis_name=None,
                  job_options=dict(queue='long.q', n_threads=1)):
        """Build a Freesurfer recon-all command for later execution.

        Parameters
        ----------
        subject : subject ID (str) | list of subject IDs (str) | 'all'
            Name (ID) of subject as a string. Both number and 3-character
            code must be given. Multiple subjects IDs can be passed as a list.
            The string 'all' is interpreted as all included subjects (i.e.,
            those that are not excluded) in the database.
        directives : str | list or str
            The tasks for recon-all to run. Run `recon-all -help` for list of
            options. Multiple options can be specified as a list of strings.
            Defaults to a list containing 'all' for the full cortical
            reconstruction pipeline and '3T' for a non-uniformity correction
            based on N3 (Zheng et al. NeuroImage, 2009) and special 3T atlas
            for Talairach alignment (aka -schwartzya3t-atlas).
        t1_series : str | None
            The name of the T1-weighted MR series to use for cortex extraction.
            This parameter is optional, it only has an effect when running
            recon-all for the first time (mri_convert from DICOM to mgz). If
            None, the value given at object creation time will be used.
        analysis_name : str | None (optional)
            Optional suffix to add to subject name (e.g. '_t2mask')
        hemi : str (optional)
            Defaults to 'both'. You may also specify either 'lh' or 'rh'.
        job_options : dict
            Dictionary of optional arguments to pass to ClusterJob. The
            default set of options is:
                job_options=dict(queue='long.q', n_threads=1)
            which sends the job to the cluster queue 'long.q', specifies that
            a single CPU core should be used (not all queues support multi-
            threading).
        """
        if isinstance(subject, (list, tuple)):
            self.logger.info('Processing multiple subjects:')
            subjects = subject
        elif isinstance(subject, string_types):
            if subject == 'all':
                self.logger.info('Processing all included subjects:')
                subjects = self.info['valid_subjects']
            else:
                subjects = [subject]

        if not isinstance(directives, (string_types, list)):
            raise RuntimeError(
                'Directives should be str or list of str, not '
                '{0}'.format(type(directives)))
        # This has the dual effect of: i) making a list out of a string, and
        # ii) COPYING the directives-list to another one
        recon_all_flags = list(directives)

        for sub in subjects:
            self.logger.info(sub)
            try:
                self._recon_all(sub, directives=recon_all_flags,
                                hemi=hemi, t1_series=t1_series,
                                analysis_name=analysis_name,
                                job_options=job_options)
            except:
                self._joblist = []  # evicerate on error
                raise

        self.logger.info('{} jobs created successfully, ready to submit.'
                         .format(len(self._joblist)))

    def _recon_all(self, subject, t1_series=None, hemi='both',
                   directives='all', analysis_name=None,
                   job_options=dict(queue='long.q', n_threads=1)):
        "Method for single subjects"

        if subject not in self.info['valid_subjects']:
            raise RuntimeError(
                'Subject {0} not found in database!'.format(subject))

        if analysis_name is not None:
            if not isinstance(analysis_name, string_types):
                raise ValueError('Analysis name suffix must be a string.')
            subject += analysis_name
        cur_subj_dir = os.path.join(self.info['subjects_dir'], subject)

        # Build command, force subjects_dir on cluster nodes
        cmd = ('recon-all -subjid {}'.format(subject) +
               ' -sd {}'.format(self.info['subjects_dir']))

        # has DICOM conversion been performed?
        if not os.path.exists(cur_subj_dir) or not check_source_readable(
                os.path.join(cur_subj_dir, 'mri', 'orig', '001.mgz')):
            if t1_series is None:
                if 't1_series' not in self.info.keys():
                    raise RuntimeError('Name of T1 series must be defined!')
                else:
                    t1_series = self.info['t1_series']

            self.logger.info('Initialising freesurfer folder structure and '
                             'converting DICOM files; this should take about '
                             '15 seconds...')
            series = _get_unique_series(Query(self.proj_name), t1_series,
                                        subject, 'MR')
            tmpdir = make_copy_of_dicom_dir(series[0]['path'])
            first_dicom = first_file_in_dir(tmpdir)
            conv_cmd = cmd + ' -i {}'.format(first_dicom)
            try:
                subp.check_output([conv_cmd], stderr=subp.STDOUT, shell=True)
            except subp.CalledProcessError as cpe:
                raise RuntimeError('Conversion failed with error message: '
                                   '{:s}'.format(cpe.returncode, cpe.output))
            finally:
                shutil.rmtree(tmpdir)
            self.logger.info('...done converting.')

        if hemi != 'both':
            if hemi not in ['lh', 'rh']:
                raise ValueError("Hemisphere must be 'lh' or 'rh'.")
            cmd += ' -hemi {0}'.format(hemi)

        cmd += ' -{}'.format(' -'.join(directives))
        self.add_job(cmd, job_name='recon-all', **job_options)

    # def apply_to_subjects(self, subjects='all', method='recon_all',
    #                       method_args=None):
    #     """Apply a Freesufer-method to a list of subjects.
    #
    #     Parameters
    #     ----------
    #     subjects : list of str | str
    #         List of subjects to loop over. If 'all', all included subjects are
    #         selected from the database (default).
    #     method : str
    #         Name of Freesurfer-method to apply. Default: 'recon_all'
    #     method_args : dict | None
    #         Dictionary of argument value-pairs to pass on to method. If None,
    #         default values of the method are used.
    #     """
    #     if isinstance(subjects, string_types) and subjects == 'all':
    #         subjects = self.info['valid_subjects']
    #     elif not isinstance(subjects, (list, tuple)):
    #         raise ValueError("Specify either list of subjects, or 'all'.")
    #     args, kwargs = parse_arguments(eval('self.' + method))
    #
    #     for sub in subjects:
    #         if not isinstance(sub, string_types):
    #             raise ValueError('Each subject name must be given as string.')
    #         cmd = 'self.' + method + "('{0}'".format(sub)
    #         for k, v in kwargs.iteritems():
    #             if method_args is not None and k in method_args.keys():
    #                 v = method_args[k]
    #
    #             if isinstance(v, string_types):
    #                 cmd += ", {0}='{1}'".format(k, v)
    #             else:
    #                 cmd += ', {0}={1}'.format(k, v)
    #
    #         cmd += ')'
    #         eval(cmd)
    #
    #     self.logger.info(
    #         'Successfully prepared {0} jobs.'.format(len(subjects)))
