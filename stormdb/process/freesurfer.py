"""
=========================
Classes related to Freesurfer
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)
import os
from six import string_types

from ..base import (enforce_path_exists, check_source_readable,
                    parse_arguments)
from ..access import Query
from ..cluster import ClusterBatch


class Freesurfer(ClusterBatch):
    """ Object for running Freesurfer in the StormDB environment

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

        self.verbose = verbose
        self.info = dict(valid_subjects=Query(proj_name).get_subjects())

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
                subjects_dir = os.path.join('/projects', proj_name,
                                            subjects_dir)

        enforce_path_exists(subjects_dir)
        self.info.update(subjects_dir=subjects_dir)

        if t1_series is not None:
            self.info.update(t1_series=t1_series)

        # Consider placing other vars here

    def recon_all(self, subject, t1_series=None, hemi='both',
                  directive='all', queue='long.q', n_threads=1,
                  recon_bin='/usr/local/freesurfer/bin/recon-all'):
        """Build a Freesurfer recon-all command for later execution.

        Parameters
        ----------
        subject : str
            Name (ID) of subject as a string. Both number and 3-character
            code must be given.
        directive : str
            The tasks for recon-all to run; default to 'all'. Run
            `recon-all -help` for list of options.
        t1_series : str | None
            The name of the T1-weighted MR series to use for cortex extraction.
            This parameter is optional, it only has an effect when running
            recon-all for the first time (mri_convert from DICOM to mgz). If
            None, the value given at object creation time will be used.
        hemi : str (optional)
            Defaults to 'both'. You may also specify either 'lh' or 'rh'.
        queue : str (optional)
            Cluster queue to submit the jobs to (default: 'long.q').
        n_threads : int (optional)
            Number of parallel CPU cores to request for the job; default is 1.
            NB: not all queues support multi-threaded execution.
        recon_bin : str (optional)
            Path to `recon-all` executable.
        """

        if subject not in self.info['valid_subjects']:
            raise RuntimeError(
                'Subject {0} not found in database!'.format(subject))
        cur_subj_dir = os.path.join(self.info['subjects_dir'], subject)

        # Start building command, force subjects_dir on cluster nodes
        cmd = (recon_bin +
               ' -{0} -subjid {1}'.format(directive, subject) +
               ' -sd {0}'.format(self.info['subjects_dir']))

        if hemi != 'both':
            if hemi not in ['lh', 'rh']:
                raise ValueError("Hemisphere must be 'lh' or 'rh'.")
            cmd += ' -hemi {0}'.format(hemi)

        # has DICOM conversion been performed?
        if not os.path.exists(cur_subj_dir) or not check_source_readable(
                os.path.join(cur_subj_dir, 'mri', 'orig', '001.mgz')):
            if t1_series is None:
                if 't1_series' not in self.info.keys():
                    raise RuntimeError('Name of T1 series must be defined!')
                else:
                    t1_series = self.info['t1_series']

            series = Query(self.proj_name).filter_series(description=t1_series,
                                                         subjects=subject,
                                                         modalities="MR")
            if len(series) == 0:
                raise RuntimeError('No series found matching {0} for subject '
                                   '{1}'.format(t1_series, subject))
            elif len(series) > 1:
                print('Multiple series match the target:')
                print([s['seriename'] for s in series])
                raise RuntimeError('More than one MR series found that '
                                   'matches the pattern {0}'.format(t1_series))
            dicom_path = os.path.join(series[0]['path'], series[0]['files'][0])
            cmd += ' -i {dcm_pth:s}'.format(dcm_pth=dicom_path)

        self.add_job(cmd, queue=queue, n_threads=n_threads,
                     job_name='recon-all')

    def apply_to_subjects(self, subjects='all', method='recon_all',
                          method_args=None):
        """Apply a Freesufer-method to a list of subjects.

        Parameters
        ----------
        subjects : list of str | str
            List of subjects to loop over. If 'all', all included subjects are
            selected from the database (default).
        method : str
            Name of Freesurfer-method to apply. Default: 'recon_all'
        method_args : dict | None
            Dictionary of argument value-pairs to pass on to method. If None,
            default values of the method are used.
        """
        if isinstance(subjects, string_types) and subjects == 'all':
            subjects = self.info['valid_subjects']
        elif not isinstance(subjects, (list, tuple)):
            raise ValueError("Specify either list of subjects, or 'all'.")
        args, kwargs = parse_arguments(eval('self.' + method))

        for sub in subjects:
            if not isinstance(sub, string_types):
                raise ValueError('Each subject name must be given as string.')
            cmd = 'self.' + method + "('{0}'".format(sub)
            for k, v in kwargs.iteritems():
                if method_args is not None and k in method_args.keys():
                    v = method_args[k]

                if isinstance(v, string_types):
                    cmd += ", {0}='{1}'".format(k, v)
                else:
                    cmd += ', {0}={1}'.format(k, v)

            cmd += ')'
            eval(cmd)

        self.logger.info(
            'Successfully prepared {0} jobs.'.format(len(subjects)))
