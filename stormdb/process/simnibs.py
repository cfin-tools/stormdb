"""
=========================
Classes related to SimNIBS

http://www.simnibs.de
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)
import os
from six import string_types
from warnings import warn
from .utils import convert_dicom_to_nifti
from ..base import (enforce_path_exists, check_source_readable,
                    _get_unique_series, mkdir_p)
from ..access import Query
from ..cluster import ClusterBatch


class SimNIBS(ClusterBatch):
    """ Object for running SimNIBS in the StormDB environment.

    NB! You must make sure that SimNIBS is enabled in your environment.
    The easiest way to achieve this is to add the following line to ~/.bashrc:
        use simnibs

    Note that SimNIBS "prepares" the T1-image fed into Freesurfer, before
    calling `recon-all`, using the T2-weighted image to mask some of the dura.
    It's therefore best to let `mri2mesh` deal with cortex extraction. If you
    want the non-modified Freesurfer-approach, see
    :class:`.freesurfer.Freesurfer` for details.

    Example 1: Create the meshes required for source space analysis of MEG,
    based on a fat-saturated T1 and high bandwidth T2 (note use of wildcards)
        >>> from stormdb.process import SimNIBS  # doctest: +SKIP
        >>> sn = SimNIBS(output_dir='scratch/sn_subjects_dir')  # doctest: +SKIP
        >>> sn.mri2mesh('0002_M55', t1_fs='*t1*FS', t2_hb='*t2*H*')  # doctest: +SKIP
        >>> sn.submit()  # doctest: +SKIP

    Example 2: Once the meshes have been calculated, convert them to lower-
    resolution surfaces suitable for BEM-based forward modeling
        >>> from stormdb.process import SimNIBS  # doctest: +SKIP
        >>> sn = SimNIBS(output_dir='scratch/sn_subjects_dir')  # doctest: +SKIP
        >>> sn.create_bem_surfaces('0002_M55', t1_fs='*t1*FS',
                                   t2_hb='*t2*H*')  # doctest: +SKIP
        >>> sn.submit()  # doctest: +SKIP

    Parameters
    ----------
    proj_name : str | None
        The name of the project. If None, will read MINDLABPROJ from
        environment.
    output_dir : str | None
        Path to place SimNIBS output in. You may also specify the path
        relative to the project directory (e.g. 'scratch/sn_subjects_dir').
        The path will be created if it does not exists.
        `mri2mesh` output is placed in output_dir/m2m_*, whereas
        `recon-all` output goes into output_dir/fs_* (* refers to a subject).
        If None, we'll try to read the environment variable SN_SUBJECTS_DIR
        from the shell (default).
    verbose : bool
        If True, print out extra information as we go (default: False).

    Attributes
    ----------
    info : dict
        See `SimNIBS().info.keys()` for contents.
    """
    def __init__(self, proj_name=None, output_dir=None, verbose=False):
        super(SimNIBS, self).__init__(proj_name, verbose=verbose)

        if output_dir is None:
            if 'SN_SUBJECTS_DIR' in os.environ.keys():
                output_dir = os.environ['SN_SUBJECTS_DIR']
            else:
                raise ValueError(
                    'No SN_SUBJECTS_DIR defined! You must do so either by '
                    'passing output_dir to this method, or by setting the '
                    'SN_SUBJECT_DIR environment variable. The directory must '
                    'exist.')
        else:
            output_dir = self._get_absolute_path(output_dir)

        enforce_path_exists(output_dir)

        self.info = dict(valid_subjects=Query(proj_name).get_subjects())
        self.info.update(output_dir=output_dir)
        self.verbose = verbose

    def mri2mesh(self, subject, t1_fs='*t1*', t2_hb='*t2*',
                 directives=['brain', 'subcort', 'head'],
                 analysis_name=None, t2mask=False, t2pial=False,
                 t1_hb=None, t2_fs=None, link_to_fs_dir=None,
                 job_options=dict(queue='long.q', n_threads=1)):
        """Build a SimNIBS mri2mesh-command for later execution.

        Parameters
        ----------
        subject : subject ID (str) | list of subject IDs (str) | 'all'
            Name (ID) of subject as a string. Both number and 3-character
            code must be given. Multiple subjects IDs can be passed as a list.
            The string 'all' is interpreted as all included subjects (i.e.,
            those that are not excluded) in the database.
        t1_fs : str
            The name of the T1-weighted & fat staturation-enabled MR series to
            use for surface creation. The name may contain wildcards, such as
            '*t1*': this will find a series that contains 't1' in its name.
            If the name contains the string '/' or '.nii', it will be treated
            as a Nifti-file. Otherwise, a dicom-to-nifti conversion will be
            performed on the corresponding series in the database.
        t2_hb : str
            The name of the T2-weighted High Bandwidth MR series to
            use for surface creation. Same logic applies as for 'tq_fs'.
        directives : str | list of str
            Directives to pass to `mri2mesh`; e.g., 'brain' -> --brain
            Multiple directives may be passed as a list. The default is:
            ['brain', 'subcort', 'head'], which is suitable for BEM creation.
        analysis_name : str | None (optional)
            Optional suffix to add to subject name (e.g. '_t2mask')
        t2mask : boo (optional)
            Tell mri2mesh to use the (high bandwidth) T2 image to mask out
            some dura on the T1 (fs) before running recon-all.
        t2pial : bool (optional)
            Tell recon-all to use the T2 image to improve extraction. NB:
            comments in mri2mesh indicate that this only works well when the
            T2 is high-res (ca. 1 mm isotropic). Consider t2mask instead.
        t1_hb : str (optional)
            The name of the T1-weighted High Bandwidth MR series to
            use for surface creation. Optional: may also be defined later.
        t2_fs : str (optional)
            The name of the T2-weighted & fat staturation-enabled MR series to
            use for surface creation. Optional: may also be defined later.
        link_to_fs_dir : str | None (optional)
            Optionally specify a path into which the Freesurfer-reconstructions
            will be linked to (default: None). This is recommended for MNE
            and mne-python users, as those tools assume this structure.
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
        for sub in subjects:
            self.logger.info(sub)
            try:
                self._mri2mesh(sub, t1_fs=t1_fs, t2_hb=t2_hb,
                               directives=directives,
                               analysis_name=analysis_name,
                               t2mask=t2mask, t2pial=t2pial, t1_hb=t1_hb,
                               t2_fs=t2_fs, link_to_fs_dir=link_to_fs_dir,
                               job_options=job_options)
            except:
                self._joblist = []  # evicerate on error
                raise

        self.logger.info('{} jobs created successfully, ready to submit.'
                         .format(len(self._joblist)))

    def _mri2mesh(self, subject, t1_fs='*t1*', t2_hb='*t2*',
                  directives=['brain', 'subcort', 'head'],
                  analysis_name=None, t2mask=False, t2pial=False,
                  t1_hb=None, t2_fs=None, link_to_fs_dir=None,
                  job_options=dict(queue='long.q', n_threads=1)):
        "Method for single subjects"

        if subject not in self.info['valid_subjects']:
            raise RuntimeError(
                'Subject {0} not found in database!'.format(subject))

        if isinstance(link_to_fs_dir, string_types):
            link_to_fs_dir = self._get_absolute_path(link_to_fs_dir)
            enforce_path_exists(link_to_fs_dir)
            if os.path.exists(os.path.join(link_to_fs_dir, subject)):
                raise RuntimeError(
                    'The directory {} already contains the subject-folder {}.'
                    '\nYou must manually (re)move it before proceeding.'
                    .format(link_to_fs_dir, subject))
            fs_dir, m2m_dir = self._mri2mesh_output_dirs(subject,
                                                         analysis_name)
            link_name = os.path.join(link_to_fs_dir, subject)
            link_cmd = 'ln -s {} {}'.format(fs_dir, link_name)

        if not isinstance(directives, (string_types, list)):
            raise RuntimeError(
                'Directive should be str or list of str, not '
                '{0}'.format(type(directives)))
        # This has the dual effect of: i) making a list out of a string, and
        # ii) COPYING the directives-list to another one
        mri2mesh_flags = list(directives)

        if t2mask and t2pial:
            raise ValueError('t2mask and t2pial cannot be used together!')
        if t2mask:
            mri2mesh_flags.append('t2mask')
        if t2pial:
            mri2mesh_flags.append('t2pial')

        # build directive string
        directives_str = ' --' + ' --'.join(mri2mesh_flags)

        # mri2mesh assumes following fixed order!
        mr_inputs = (t1_hb, t1_fs, t2_hb, t2_fs)
        mr_inputs_str = ''
        for mri in mr_inputs:
            if mri is not None and '/' not in mri and '.nii' not in mri:
                series = _get_unique_series(Query(self.proj_name), mri,
                                            subject, 'MR')
                dcm = os.path.join(series[0]['path'], series[0]['files'][0])
                nii_path = os.path.join(self.info['output_dir'], 'nifti',
                                        subject)
                mkdir_p(nii_path)
                mri = series[0]['seriename']  # in case wildcards were used
                mri = os.path.join(nii_path, mri + '.nii.gz')
                if not os.path.isfile(mri):  # if exists, don't redo!
                    self.logger.info('Converting DICOM to Nifti, this will '
                                     'take about 15 seconds...')
                    convert_dicom_to_nifti(dcm, mri)
                    self.logger.info('...done.')
                else:
                    if self.verbose:
                        print('The file {:s} already exists: will use '
                              'it instead of re-converting.'.format(mri))
                    else:
                        warn('Some input files already exist in {:s}; these '
                             'will be used instead of re-converting.'
                             .format(nii_path))

            if mri is not None:
                mr_inputs_str += ' ' + mri

        if analysis_name is not None:
            if not isinstance(analysis_name, string_types):
                raise ValueError('Analysis name suffix must be a string.')
            subject += analysis_name

        # Build command
        cmd = ['mri2mesh ' + directives_str + ' ' + subject + mr_inputs_str]

        if link_to_fs_dir is not None:
            cmd += [link_cmd]

        self.add_job(cmd, working_dir=self.info['output_dir'],
                     job_name='mri2mesh', **job_options)

    def create_bem_surfaces(self, subject, n_vertices=5120,
                            analysis_name=None,
                            job_options=dict(queue='short.q', n_threads=1)):
        """Convert mri2mesh output to Freesurfer meshes suitable for BEMs.

        Parameters
        ----------
        subject : str
            Name (ID) of subject as a string. Both number and 3-character
            code must be given.
        n_vertices : int
            Number of vertices to subsample the high-resolution surfaces to
            (default: 5120).
        analysis_name : str | None
            Optional suffix to add to subject name (e.g. '_with_t2mask')
        job_options : dict
            Dictionary of optional arguments to pass to ClusterJob. The
            default set here is: job_options=dict(queue='short.q', n_threads=1)
        """
        if subject not in self.info['valid_subjects']:
            raise RuntimeError(
                'Subject {0} not found in database!'.format(subject))

        fs_dir, m2m_dir = self._mri2mesh_output_dirs(subject, analysis_name)
        try:
            enforce_path_exists(fs_dir)
            enforce_path_exists(m2m_dir)
        except IOError as m2m_err:
            msg = ('{0}\nFailed to find accessible mri2mesh-folders; '
                   'did it complete successfully?'.format(m2m_err))
            if isinstance(analysis_name, string_types):
                msg += ('\nPlease also check that the analysis_name is '
                        'correct: {0}'.format(analysis_name))
            raise RuntimeError(msg)

        meshfix_opts = ' -u 10 --vertices {:d} --fsmesh'.format(n_vertices)
        bem_dir = os.path.join(fs_dir, 'bem')
        bem_surfaces = dict(inner_skull='csf.stl',
                            outer_skull='skull.stl',
                            outer_skin='skin.stl')
        for bem_layer, surf in bem_surfaces.items():
            surf_fname = os.path.join(m2m_dir, surf)
            if not check_source_readable(surf_fname):
                raise RuntimeError(
                    'Could not find surface {surf:s}; mri2mesh may have exited'
                    ' with an error, please check.'.format(surf=surf))
            bem_fname = os.path.join(bem_dir, bem_layer)

            cmds = ['meshfix {sfn:s} {mfo:s} -o {bfn:s}'
                    .format(sfn=surf_fname, mfo=meshfix_opts, bfn=bem_fname)]

            xfm_volume = os.path.join(m2m_dir, 'tmp', 'subcortical_FS.nii.gz')
            xfm = os.path.join(m2m_dir, 'tmp', 'unity.xfm')

            # NB This is needed! Otherwise the stl->fsmesh conversion output
            # lacks some transformation and is misaligned with the MR
            cmds += ['mris_transform --dst {xv:s} --src {xv:s} '
                     '{bfn:s}.fsmesh {xfm:s} {bfn:s}.surf'
                     .format(xv=xfm_volume, bfn=bem_fname, xfm=xfm)]
            cmds += ['rm {bfn:s}.fsmesh'.format(bfn=bem_fname)]

        # One job per subject, since these are "cheap" operations
        self.add_job(cmds, job_name='meshfix',
                     working_dir=self.info['output_dir'],
                     **job_options)

    def _mri2mesh_output_dirs(self, subject, analysis_name):
        if analysis_name is not None:
            suffix = analysis_name
        else:
            suffix = ''

        fs_dir = os.path.join(self.info['output_dir'],
                              'fs_' + subject + suffix)
        m2m_dir = os.path.join(self.info['output_dir'],
                               'm2m_' + subject + suffix)

        return fs_dir, m2m_dir

    def _get_absolute_path(self, output_dir):
        if not output_dir.startswith('/'):
            # the path can be _relative_ to the project dir
            output_dir = os.path.join('/projects', self.proj_name,
                                      output_dir)
        return output_dir
