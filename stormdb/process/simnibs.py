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
                    parse_arguments, _get_unique_series, mkdir_p)
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
            if not output_dir.startswith('/'):
                # the path can be _relative_ to the project dir
                output_dir = os.path.join('/projects', self.proj_name,
                                          output_dir)

        enforce_path_exists(output_dir)

        self.info = dict(valid_subjects=Query(proj_name).get_subjects())
        self.info.update(output_dir=output_dir)
        self.verbose = verbose

    def mri2mesh(self, subject, t1_fs=None, t2_hb=None,
                 directive=['brain', 'subcort', 'head'],
                 analysis_name=None,
                 t1_hb=None, t2_fs=None, t2mask=False, t2pial=False,
                 simnibs_dir='/usr/local/simnibs',
                 queue='long.q', n_threads=1):

        """Build a SimNIBS mri2mesh-command for later execution.

        Parameters
        ----------
        subject : str
            Name (ID) of subject as a string. Both number and 3-character
            code must be given.
        t1_fs : str
            The name of the T1-weighted & fat staturation-enabled MR series to
            use for surface creation.
            If the name contains the string '/' or '.nii', it will be treated
            as a Nifti-file. Otherwise, a dicom-to-nifti conversion will be
            performed on the corresponding series in the database.
        t2_hb : str
            The name of the T2-weighted High Bandwidth MR series to
            use for surface creation.
        directive : str | list of str
            Directive to pass to `mri2mesh`; e.g., 'brain' -> --brain
            Multiple directives may be passed as a list.
        analysis_name : str | None
            Optional suffix to add to subject name (e.g. '_with_t2mask')
        t2mask : bool
            Tell mri2mesh to use the (high bandwidth) T2 image to mask out
            some dura on the T1 (fs) before running recon-all.
        t2pial : bool
            Tell recon-all to use the T2 image to improve extraction. NB:
            comments in mri2mesh indicate that this only works well when the
            T2 is high-res (ca. 1 mm isotropic). Consider t2mask instead.
        t1_hb : str (optional)
            The name of the T1-weighted High Bandwidth MR series to
            use for surface creation. Optional: may also be defined later.
        t2_fs : str (optional)
            The name of the T2-weighted & fat staturation-enabled MR series to
            use for surface creation. Optional: may also be defined later.
        queue : str (optional)
            Cluster queue to submit the jobs to (default: 'long.q').
        n_threads : int (optional)
            Number of parallel CPU cores to request for the job; default is 1.
            NB: not all queues support multi-threaded execution.
        """
        if subject not in self.info['valid_subjects']:
            raise RuntimeError(
                'Subject {0} not found in database!'.format(subject))

        if not isinstance(directive, (string_types, list)):
            raise RuntimeError(
                'Directive should be str or list of str, not '
                '{0}'.format(type(directive)))
        if isinstance(directive, string_types):
            directive = [directive]

        if t2mask and t2pial:
            raise ValueError('t2mask and t2pial cannot be used together!')
        if t2mask:
            directive.append('t2mask')
        if t2pial:
            directive.append('t2pial')

        # build directive string
        directives_str = ' --' + ' --'.join(directive)

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
                mri = os.path.join(nii_path, subject + '_' + mri + '.nii.gz')
                if not os.path.isfile(mri):  # if exists, don't redo!
                    self.logger.info('Converting DICOM to Nifti, this will '
                                     'take about 15 seconds...')
                    convert_dicom_to_nifti(dcm, mri)
                    self.logger.info('...done.')
                else:
                    warn('The file {:s} already exists: will use '
                         'it instead of re-converting.'.format(mri))

            if mri is not None:
                mr_inputs_str += ' ' + mri

        if analysis_name is not None:
            if not isinstance(analysis_name, string_types):
                raise ValueError('Analysis name suffix must be a string.')
            subject += analysis_name

        # Build command
        cmd = 'mri2mesh ' + directives_str + ' ' + subject + mr_inputs_str

        self.add_job(cmd, queue=queue, n_threads=n_threads,
                     job_name='mri2mesh', working_dir=self.info['output_dir'])

# def make_symbolic_links(subject, subjects_dir):
#     """Make symblic links between FS dir and subjects_dir.
#     Parameters
#     ----------
#     fname : string
#         The name of the subject to create for
#     subjects_dir : string
#         The subjects dir for FreeSurfer
#     """
#
#     make_links = "ln -s fs_%s/* ." % subject[:4]
#     os.chdir(fs_subjects_dir + subject[:4])
#     subprocess.call([cmd, "1", make_links])
#
#
# def convert_surfaces(subject, subjects_dir):
#     """Convert the SimNIBS surface to FreeSurfer surfaces.
#     Parameters
#     ----------
#     subject : string
#        The name of the subject
#     subjects_dir : string
#         The subjects dir for FreeSurfer
#     """
#     convert_csf = "meshfix csf.stl -u 10 --vertices 4098 --fsmesh"
#     convert_skull = "meshfix skull.stl -u 10 --vertices 4098 --fsmesh"
#     convert_skin = "meshfix skin.stl -u 10 --vertices 4098 --fsmesh"
#
#     os.chdir(fs_subjects_dir + subject[:4] + "/m2m_%s" % subject[:4])
#     subprocess.call([cmd, "1", convert_csf])
#     subprocess.call([cmd, "1", convert_skull])
#     subprocess.call([cmd, "1", convert_skin])
#
#
# def copy_surfaces(subject, subjects_dir):
#     """Copy the converted FreeSurfer surfaces to the bem dir.
#     Parameters
#     ----------
#     subject : string
#        The name of the subject
#     subjects_dir : string
#         The subjects dir for FreeSurfer
#     """
#     os.chdir(fs_subjects_dir + subject[:4] + "/m2m_%s" % subject[:4])
#     copy_inner_skull = "cp -f csf_fixed.fsmesh " + subjects_dir + \
#                        "/%s/bem/inner_skull.surf" % subject[:4]
#     copy_outer_skull = "cp -f skull_fixed.fsmesh " + subjects_dir + \
#                        "/%s/bem/outer_skull.surf" % subject[:4]
#     copy_outer_skin = "cp -f skin_fixed.fsmesh " + subjects_dir + \
#                        "/%s/bem/outer_skin.surf" % subject[:4]
#
#     subprocess.call([cmd, "1", copy_inner_skull])
#     subprocess.call([cmd, "1", copy_outer_skull])
#     subprocess.call([cmd, "1", copy_outer_skin])
#
#     os.chdir(fs_subjects_dir + subject[:4] + "/bem")
#     convert_skin_to_head = "mne_surf2bem --surf outer_skin.surf --fif %s-head.fif --id 4" % subject[:4]
#     subprocess.call([cmd, "1", convert_skin_to_head])
#
#
# def setup_mne_c_forward(subject):
#     setup_forward = "mne_setup_forward_model --subject %s --surf --ico -6" %subject[:4]
#     subprocess.call([cmd, "1", setup_forward])
#
#
# for subject in included_subjects[3:5]:
#     make_symbolic_links(subject, fs_subjects_dir)
#
# for subject in included_subjects[3:5]:
#     convert_surfaces(subject, fs_subjects_dir)
#
# for subject in included_subjects[3:5]:
#     copy_surfaces(subject, fs_subjects_dir)
#
# for subject in included_subjects[3:5]:
#     setup_mne_c_forward(subject)
