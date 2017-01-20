"""
=========================
Classes related to Freesurfer
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)
import os
import os.path as op
import subprocess as subp
import shutil
import glob

from six import string_types
from warnings import warn

from .utils import first_file_in_dir, make_copy_of_dicom_dir
from ..base import (enforce_path_exists, check_source_readable,
                    _get_unique_series, add_to_command)
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
                                   '{:s}'.format(cpe.output))
            finally:
                shutil.rmtree(tmpdir)
            self.logger.info('...done converting.')

        if hemi != 'both':
            if hemi not in ['lh', 'rh']:
                raise ValueError("Hemisphere must be 'lh' or 'rh'.")
            cmd += ' -hemi {0}'.format(hemi)

        cmd += ' -{}'.format(' -'.join(directives))
        self.add_job(cmd, job_name='recon-all', **job_options)

    def create_bem_surfaces(self, subject, analysis_name=None,
                            flash5=None, flash30=None, make_coreg_head=True,
                            job_options=dict(queue='short.q', n_threads=1),
                            **kwargs):
        """Convert mri2mesh output to Freesurfer meshes suitable for BEMs.

        Parameters
        ----------
        subject : subject ID (str) | list of subject IDs (str) | 'all'
            Name (ID) of subject as a string. Both number and 3-character
            code must be given. Multiple subjects IDs can be passed as a list.
            The string 'all' is interpreted as all included subjects (i.e.,
            those that are not excluded) in the database.
        flash5 : str | None (optional)
            The name of the multi-echo FLASH series with 5 degree flip angle
            that will be used to create the 3 main compartments of the head:
            inner skull, outer skull and outer skin. If None (default), the
            watershed-algorithm in Freesurfer will be used to create the inner
            skull surface only, though results vary.
        flash30 : str | None (optional)
            The name of the multi-echo FLASH series with 30 degree flip angle.
            If None (default), only the 5 degree FLASH will be used. The
            difference in quality of the 3 layers extracted is minor.
        make_coreg_head : bool
            If True (default), make a high-resolution head (outer skin) surface
            for MEG/EEG coregistration purposes. NB: The number of vertices is
            currently hard-coded to be 20,000; this is arbitrary though, and
            could be made more or less depending on needs.
        analysis_name : str | None
            Optional suffix to add to subject name (e.g. '_with_t2mask')
        job_options : dict
            Dictionary of optional arguments to pass to ClusterJob. The
            default set here is: job_options=dict(queue='short.q', n_threads=1)
        **kwargs : optional
            Optional keyword arguments, depending on whether FLASH-images or
            watershed-algorithm is used.
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

        do_watershed, do_flash = False, False
        if flash5 is None and flash30 is not None:
            raise ValueError('To use FLASH 30, FLASH 5 must be defined.')
        elif flash5 is None and flash30 is None:
            do_watershed = True
        elif flash5 is not None:
            if not isinstance(flash5, string_types):
                raise ValueError('flash5 must be a series name (str)')
            if flash30 is not None and not isinstance(flash30, string_types):
                raise ValueError('flash30 must be a series name (str)')
            do_flash = True

        for sub in subjects:
            self.logger.info(sub)
            if subject not in self.info['valid_subjects']:
                raise RuntimeError(
                    'Subject {0} not found in database!'.format(subject))
            cur_subj_dir = os.path.join(self.info['subjects_dir'], subject)
            try:
                enforce_path_exists(cur_subj_dir)
            except IOError as err:
                msg = ('{0}\nFailed to find accessible Freesurfer output; '
                       'did it complete successfully?'.format(err))
                if isinstance(analysis_name, string_types):
                    msg += ('\nPlease also check that the analysis_name is '
                            'correct: {0}'.format(analysis_name))
                raise RuntimeError(msg)

            try:
                if do_flash:
                    self._create_bem_surfaces_flash(
                        sub, flash5, flash30=flash30,
                        make_coreg_head=make_coreg_head,
                        analysis_name=analysis_name,
                        job_options=job_options, **kwargs)
                elif do_watershed:
                    self._create_bem_surfaces_watershed(
                        sub, make_coreg_head=make_coreg_head,
                        analysis_name=analysis_name,
                        job_options=job_options, **kwargs)
            except:
                self._joblist = []  # evicerate on error
                raise

        self.logger.info('{} jobs created successfully, ready to submit.'
                         .format(len(self._joblist)))

    def _create_bem_surfaces_flash(self, subject, flash5, make_coreg_head=True,
                                   analysis_name=None, flash30=None,
                                   job_options=dict(queue='short.q',
                                                    n_threads=1)):
        """Create BEMs for single subject."""
        subject_dirname = subject
        if analysis_name is not None:
            subject_dirname += analysis_name

        cmd = None

        series = _get_unique_series(Query(self.proj_name), flash5,
                                    subject, 'MR')
        flash5_name = '{:03d}_{:s}'.format(int(series[0]['serieno']),
                                           series[0]['seriename'])
        bem_dir = op.join(self.info['subjects_dir'], subject_dirname, 'bem')
        mri_dir = op.join(self.info['subjects_dir'], subject_dirname, 'mri')
        flash_dir = op.join(mri_dir, 'flash')
        flash_dcm = op.join(flash_dir, 'dicom')  # same for 5 and 30!

        if op.isdir(flash_dcm):
            warn('Copy of FLASH image DICOMs found. Submitting this script '
                 'will overwrite previous surfaces.')

        cmd = add_to_command(cmd, 'mkdir -p {}', flash_dcm)
        cmd = add_to_command(cmd, 'cp {}/* {}', series[0]['path'], flash_dcm)

        if flash30 is not None:
            series = _get_unique_series(Query(self.proj_name), flash30,
                                        subject, 'MR')
            flash30_name = series[0]['seriename']
            cmd = add_to_command(cmd, 'cp {}/* {}',
                                 series[0]['path'], flash_dcm)

        cmd = add_to_command(cmd, 'cd {} && mne_organize_dicom {}',
                             flash_dir, flash_dcm)

        cmd = add_to_command(cmd, 'rm -f flash05 && ln -s {} flash05',
                             flash5_name)

        flash30_str = ' --noflash30'
        if flash30 is not None:
            cmd = add_to_command(cmd, 'ln -s {} flash30', flash30_name)
            flash30_str = ''

        cmd = add_to_command(cmd, ('cfin_flash_bem -s {sub:s} -d {subdir:s}'
                                   '{f30_str:s} --overwrite'),
                             sub=subject_dirname, f30_str=flash30_str,
                             subdir=self.info['subjects_dir'])

        # NB mne/bem.py:make_flash_bem seems to be missing this step!
        surf_names = ('inner_skull', 'outer_skull', 'outer_skin')
        flash5_reg = op.join(flash_dir, 'parameter_maps', 'flash5_reg.mgz')
        for sn in surf_names:
            tri_fname = op.join(bem_dir, sn + '.tri')
            surf_fname = op.join(bem_dir, sn + '.surf')
            cmd = add_to_command(cmd,
                                 ('mne_convert_surface --tri {tri:s} '
                                  ' --surfout {surf:s} --swap --mghmri '
                                  '{f5reg:s}'), tri=tri_fname, surf=surf_fname,
                                 f5reg=flash5_reg)
        if make_coreg_head:
            cmd = make_coreg_head_commands(bem_dir, subject_dirname, cmd=cmd)

        self.add_job(cmd, job_name='cfin_flash_bem', **job_options)

    def _create_bem_surfaces_watershed(self, subject, analysis_name=None,
                                       atlas=False, gcaatlas=True,
                                       make_coreg_head=False,
                                       job_options=dict(queue='short.q',
                                                        n_threads=1)):
        """Create inner_skull for single subject."""
        subject_dirname = subject
        if analysis_name is not None:
            subject_dirname += analysis_name

        if atlas and gcaatlas:
            raise ValueError(
                'atlas and gcaatlas cannot be used together; choose one.')
        elif atlas:
            atlas_str = '--atlas'
        elif gcaatlas:
            atlas_str = '--gcaatlas'
        else:
            atlas_str = ''

        # self.logger.info('Running mne_watershed_bem...')
        cmd = None

        # Just in case: commands below are dependent on it set in environ
        # cmd = add_to_command(cmd, 'export SUBJECTS_DIR={}',
        #                     self.info['subjects_dir'])

        # NB Using local version of mne_watershed_bem
        cmd = add_to_command(cmd, ('cfin_watershed_bem --sd {sd:s} '
                                   '--subject {sub:s} {atl:s} --overwrite'),
                             sd=self.info['subjects_dir'],
                             sub=subject_dirname, atl=atlas_str)

        bem_dir = op.join(self.info['subjects_dir'], subject_dirname, 'bem')
        surf_names = ('inner_skull', 'outer_skull', 'outer_skin')
        for sn in surf_names:
            surf_fname = op.join(bem_dir, sn + '.surf')
            cmd = add_to_command(cmd, ('ln -s watershed/{}_{}_surface {}'),
                                 subject_dirname, sn, surf_fname)

        if make_coreg_head:
            cmd = make_coreg_head_commands(bem_dir, subject_dirname, cmd=cmd)

        self.add_job(cmd, job_name='watershed_bem', **job_options)


# NB This is a modified version of that found in mne-python/mne/bem.py
# (13 Jan 2017). Some options are removed, and the number of echos can vary.
def convert_flash_mris_cfin(subject, flash30=False, n_echos=8,
                            subjects_dir=None, unwarp=False):
    """Convert DICOM files for use with make_flash_bem.

    Parameters
    ----------
    subject : str
        Subject name.
    flash30 : bool
        Use 30-degree flip angle data.
    unwarp : bool
        Run grad_unwarp with -unwarp option on each of the converted
        data sets. It requires FreeSurfer's MATLAB toolbox to be properly
        installed.
    subjects_dir : string, or None
        Path to SUBJECTS_DIR if it is not set in the environment.

    This function assumes that the Freesurfer segmentation of the subject
    has been completed. In particular, the T1.mgz and brain.mgz MRI volumes
    should be, as usual, in the subject's mri directory.
    """

    if n_echos < 5:
        raise ValueError(
            'Less than 5 echos are currently not supported.')
    elif flash30 and n_echos != 8:
        raise ValueError(
            'When using the 30 degree sequence, exactly 8 echos must be used')

    echos = ['{:03d}'.format(e) for e in range(1, n_echos + 1)]
    alt_echos = ['{:03d}'.format(e) for e in range(2, n_echos + 2)]

    env, mri_dir = _prepare_env(subject, subjects_dir,
                                requires_freesurfer=True,
                                requires_mne=False)[:2]
    curdir = os.getcwd()
    # Step 1a : Data conversion to mgz format
    if not op.exists(op.join(mri_dir, 'flash', 'parameter_maps')):
        os.makedirs(op.join(mri_dir, 'flash', 'parameter_maps'))
    echos_done = 0
    # Assume always need to convert first!
    print("\n---- Converting Flash images ----")
    # echos = ['001', '002', '003', '004', '005', '006', '007', '008']
    if flash30:
        flashes = ['05', '30']
    else:
        flashes = ['05']
    #
    missing = False
    for flash in flashes:
        for echo in echos:
            if not op.isdir(op.join('flash' + flash, echo)):
                missing = True
    if missing:
        # echos = ['002', '003', '004', '005', '006', '007', '008', '009']
        echos = alt_echos
        for flash in flashes:
            for echo in echos:
                if not op.isdir(op.join('flash' + flash, echo)):
                    raise RuntimeError("Directory %s is missing."
                                       % op.join('flash' + flash, echo))
    #
    for flash in flashes:
        for echo in echos:
            if not op.isdir(op.join('flash' + flash, echo)):
                raise RuntimeError("Directory %s is missing."
                                   % op.join('flash' + flash, echo))
            sample_file = glob.glob(op.join('flash' + flash, echo, '*'))[0]
            dest_file = op.join(mri_dir, 'flash',
                                'mef' + flash + '_' + echo + '.mgz')
            # do not redo if already present
            if op.isfile(dest_file):
                print("The file %s is already there")
            else:
                cmd = ['mri_convert', sample_file, dest_file]
                _run_subprocess(cmd, stderr=subp.STDOUT, shell=True)
                echos_done += 1
    # Step 1b : Run grad_unwarp on converted files
    os.chdir(op.join(mri_dir, "flash"))
    files = glob.glob("mef*.mgz")
    if unwarp:
        print("\n---- Unwarp mgz data sets ----")
        for infile in files:
            outfile = infile.replace(".mgz", "u.mgz")
            cmd = ['grad_unwarp', '-i', infile, '-o', outfile, '-unwarp',
                   'true']
            _run_subprocess(cmd, stderr=subp.STDOUT, shell=True)
    # Clear parameter maps if some of the data were reconverted
    if echos_done > 0 and op.exists("parameter_maps"):
        shutil.rmtree("parameter_maps")
        print("\nParameter maps directory cleared")
    if not op.exists("parameter_maps"):
        os.makedirs("parameter_maps")
    # Step 2 : Create the parameter maps
    if flash30:
        print("\n---- Creating the parameter maps ----")
        if unwarp:
            files = glob.glob("mef05*u.mgz")
        if len(os.listdir('parameter_maps')) == 0:
            cmd = ['mri_ms_fitparms'] + files + ['parameter_maps']
            _run_subprocess(cmd, stderr=subp.STDOUT, shell=True)
        else:
            print("Parameter maps were already computed")
        # Step 3 : Synthesize the flash 5 images
        print("\n---- Synthesizing flash 5 images ----")
        os.chdir('parameter_maps')
        if not op.exists('flash5.mgz'):
            cmd = ['mri_synthesize', '20 5 5', 'T1.mgz', 'PD.mgz',
                   'flash5.mgz']
            _run_subprocess(cmd, stderr=subp.STDOUT, shell=True)
            os.remove('flash5_reg.mgz')
        else:
            print("Synthesized flash 5 volume is already there")
    else:
        print("\n---- Averaging flash5 echoes ----")
        os.chdir('parameter_maps')
        if unwarp:
            files = glob.glob("../mef05*u.mgz")
        else:
            files = glob.glob("../mef05*.mgz")
        cmd = ['mri_average', '-noconform'] + files + ['flash5.mgz']
        _run_subprocess(cmd, stderr=subp.STDOUT, shell=True)
        if op.exists('flash5_reg.mgz'):
            os.remove('flash5_reg.mgz')

    # Go back to initial directory
    os.chdir(curdir)


def make_coreg_head_commands(bem_dir, subject_dirname, cmd=None):

    cmd = add_to_command(cmd, 'cd {} && mkheadsurf -subjid {}',
                         bem_dir, subject_dirname)
    cmd = add_to_command(cmd, ('mne_surf2bem --surf ../surf/lh.seghead '
                         '--id 4 --check --fif {}-head-dense.fif'),
                         subject_dirname)
    cmd = add_to_command(cmd, ('rm -f {sub:s}-head.fif && '
                         'ln -s {sub:s}-head-dense.fif {sub:s}-head.fif'),
                         sub=subject_dirname)
    return cmd


def _prepare_env(subject, subjects_dir, requires_freesurfer, requires_mne):
    """Helper to prepare an env object for subprocess calls.

    NB: Copied from mne-python/mne/bem.py !
    """
    env = os.environ.copy()
    if requires_freesurfer and not os.environ.get('FREESURFER_HOME'):
        raise RuntimeError('I cannot find freesurfer. The FREESURFER_HOME '
                           'environment variable is not set.')
    if requires_mne and not os.environ.get('MNE_ROOT'):
        raise RuntimeError('I cannot find the MNE command line tools. The '
                           'MNE_ROOT environment variable is not set.')

    if not isinstance(subject, string_types):
        raise TypeError('The subject argument must be set')

    # subjects_dir = get_subjects_dir(subjects_dir, raise_error=True)
    if not op.isdir(subjects_dir):
        raise RuntimeError('Could not find the MRI data directory "%s"'
                           % subjects_dir)
    subject_dir = op.join(subjects_dir, subject)
    if not op.isdir(subject_dir):
        raise RuntimeError('Could not find the subject data directory "%s"'
                           % (subject_dir,))
    env['SUBJECT'] = subject
    env['SUBJECTS_DIR'] = subjects_dir
    mri_dir = op.join(subject_dir, 'mri')
    bem_dir = op.join(subject_dir, 'bem')
    return env, mri_dir, bem_dir


def _run_subprocess(cmd, msg=None, **kwargs):
    if isinstance(cmd, string_types):
        cmd = [cmd]
    elif 'shell' in kwargs.keys():
        if kwargs['shell']:
            # for some reason check_output fails if cmd is not a string
            cmd = ' '.join(cmd)
    try:
        subp.check_output(cmd, **kwargs)
    except subp.CalledProcessError as cpe:
        if msg is None:
            msg = ''
        else:
            msg += '\n'
        raise RuntimeError('{:s}{:s}'.format(msg, cpe.output))
