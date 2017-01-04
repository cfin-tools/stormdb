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
from .base import (enforce_path_exists, check_source_readable, parse_arguments)
from ..access import Query
from ..cluster import ClusterBatch


class SimNIBS(ClusterBatch):
    """ Object for running SimNIBS in the StormDB environment.

    NB! You must make sure that SimNIBS is enabled in your environment.
    The easiest way to achieve this is to add the following line to ~/.bashrc:
        use simnibs

    Note that a successfull Freesurfer `recon-all -all`-reconstruction is
    a prerequisite for running SimNIBS. See :class:`.freesurfer.Freesurfer`
    for details.

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
    verbose : bool
        If True, print out extra information as we go (default: False).

    Attributes
    ----------
    info : dict
        See `SimNIBS().info.keys()` for contents.
    """
    def __init__(self, proj_name, bad=[], verbose=False):
        super(SimNIBS, self).__init__(proj_name, verbose=verbose)

        self.verbose = verbose
        self.info = dict(valid_subjects=Query(proj_name).get_subjects())

    def mri2mesh(self, t1_fs=None, t2_hb=None, t1_hb=None, t2_fs=None,
                 simnibs_dir='/usr/local/simnibs',
                 directive='all', queue='long.q', n_threads=1):

        """Build a NeuroMag MaxFilter command for later execution.

        See the Maxfilter manual for details on the different options!

        Things to implement
        * check that cal-file matches date in infile!
        * check that maxfilter binary is OK

        Parameters
        ----------
        t1_fs : str
            The name of the T1-weighted & fat staturation-enabled MR series to
            use for surface creation. Optional: may also be defined later.
        t2_hb : str
            The name of the T2-weighted High Bandwidth MR series to
            use for surface creation. Optional: may also be defined later.
        t1_hb : str (optional)
            The name of the T1-weighted High Bandwidth MR series to
            use for surface creation. Optional: may also be defined later.
        t2_fs : str (optional)
            The name of the T2-weighted & fat staturation-enabled MR series to
            use for surface creation. Optional: may also be defined later.
        n_threads : int
            Number of parallel threads to execute on (default: 4)
    """


def make_symbolic_links(subject, subjects_dir):
    """Make symblic links between FS dir and subjects_dir.
    Parameters
    ----------
    fname : string
        The name of the subject to create for
    subjects_dir : string
        The subjects dir for FreeSurfer
    """

    make_links = "ln -s fs_%s/* ." % subject[:4]
    os.chdir(fs_subjects_dir + subject[:4])
    subprocess.call([cmd, "1", make_links])


def convert_surfaces(subject, subjects_dir):
    """Convert the SimNIBS surface to FreeSurfer surfaces.
    Parameters
    ----------
    subject : string
       The name of the subject
    subjects_dir : string
        The subjects dir for FreeSurfer
    """
    convert_csf = "meshfix csf.stl -u 10 --vertices 4098 --fsmesh"
    convert_skull = "meshfix skull.stl -u 10 --vertices 4098 --fsmesh"
    convert_skin = "meshfix skin.stl -u 10 --vertices 4098 --fsmesh"

    os.chdir(fs_subjects_dir + subject[:4] + "/m2m_%s" % subject[:4])
    subprocess.call([cmd, "1", convert_csf])
    subprocess.call([cmd, "1", convert_skull])
    subprocess.call([cmd, "1", convert_skin])


def copy_surfaces(subject, subjects_dir):
    """Copy the converted FreeSurfer surfaces to the bem dir.
    Parameters
    ----------
    subject : string
       The name of the subject
    subjects_dir : string
        The subjects dir for FreeSurfer
    """
    os.chdir(fs_subjects_dir + subject[:4] + "/m2m_%s" % subject[:4])
    copy_inner_skull = "cp -f csf_fixed.fsmesh " + subjects_dir + \
                       "/%s/bem/inner_skull.surf" % subject[:4]
    copy_outer_skull = "cp -f skull_fixed.fsmesh " + subjects_dir + \
                       "/%s/bem/outer_skull.surf" % subject[:4]
    copy_outer_skin = "cp -f skin_fixed.fsmesh " + subjects_dir + \
                       "/%s/bem/outer_skin.surf" % subject[:4]

    subprocess.call([cmd, "1", copy_inner_skull])
    subprocess.call([cmd, "1", copy_outer_skull])
    subprocess.call([cmd, "1", copy_outer_skin])

    os.chdir(fs_subjects_dir + subject[:4] + "/bem")
    convert_skin_to_head = "mne_surf2bem --surf outer_skin.surf --fif %s-head.fif --id 4" % subject[:4]
    subprocess.call([cmd, "1", convert_skin_to_head])


def setup_mne_c_forward(subject):
    setup_forward = "mne_setup_forward_model --subject %s --surf --ico -6" %subject[:4]
    subprocess.call([cmd, "1", setup_forward])


for subject in included_subjects[3:5]:
    make_symbolic_links(subject, fs_subjects_dir)

for subject in included_subjects[3:5]:
    convert_surfaces(subject, fs_subjects_dir)

for subject in included_subjects[3:5]:
    copy_surfaces(subject, fs_subjects_dir)

for subject in included_subjects[3:5]:
    setup_mne_c_forward(subject)
