"""
=========================
Classes related to Freesurfer
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)
import os

from .base import (enforce_path_exists, check_source_readable)
from ..access import Query
from ..cluster import ClusterBatch


class Freesurfer(ClusterBatch):
    """ Object for running Freesurfer in the StormDB environment

    Parameters
    ----------
    proj_name : str
        The name of the project.
    subjects_dir : str | None
        Relative path to the Freesurfer SUBJECTS_DIR
        (e.g. 'scratch/fs_subjects_dir'). If None, we'll first try to read
        the corresponding environment variable from the shell (default).
    t1_series : str | None
        The name of the T1-weighted sequence to use. If None, this must be
        defined at job creation time.
    verbose : bool
        If True (default), print out a bunch of information as we go.

    Attributes
    ----------
    info : dict
        Various info
    joblist : list of ClusterJob's
        If defined, represents a sequence of freesurfer shell calls.
    """

    def __init__(self, proj_name, subjects_dir=None, t1_series=None,
                 verbose=True):
        super(Freesurfer, self).__init__(proj_name)

        self.info = dict(io_mapping=[])

        if subjects_dir is None:
            if 'SUBJECTS_DIR' in os.environ.keys():
                subjects_dir = os.environ['SUBJECTS_DIR']
            else:
                raise ValueError('No SUBJECTS_DIR defined! You must do so '
                                 'either by using an argument to this method, '
                                 'or by setting the SUBJECT_DIR environment '
                                 'variable. The directory must exist.')

        enforce_path_exists(subjects_dir)
        self.info.update(subjects_dir=subjects_dir)

        if t1_series is not None:
            self.info.update(t1_series=t1_series)

        # Consider placing other vars here

    def recon_all(self, subject, t1_series=None, hemi='both',
                  process_flag='all',
                  recon_bin='/usr/local/freesurfer/bin/recon-all',
                  logfile=None, n_threads=4):

        """Build a Freesurfer command for later execution.

        Parameters
        ----------
        in_fname : str
            Input file name
        """

        cur_subj_dir = os.path.join(self.info['subjects_dir'], subject)

        # Start building command
        cmd = recon_bin + ' -{0} -subjid {1}'.format(process_flag, subject)

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

            qy = Query(self.proj_name)
            series = qy.filter_series(description=t1_series,
                                      subjects=subject,
                                      modalities="MR")
            if len(series) == 0:
                raise RuntimeError('No series found matching {0} for subject '
                                   '{1}'.format(t1_series, subject))
            elif len(series) > 1:
                raise RuntimeError('More than one MR series found that '
                                   'matches the pattern {0}'.format(t1_series))
            dicom_path = os.path.join(series[0]['path'], series[0]['files'][0])
            cmd += ' -i {dcm_pth:s}'.format(dcm_pth=dicom_path)
