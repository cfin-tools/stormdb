from .base import check_destination_writable, check_source_readable
from ..cluster import ClusterBatch


class SimNIBS(ClusterBatch):
    """Clusterised SimNIBS pipeline.

       http://www.simnibs.de
    """
    def __init__(self, proj_name, bad=[], verbose=True):
        super(SimNIBS, self).__init__(proj_name)

        self.info = dict(bad=bad, io_mapping=[])

    def build_cmd(self, t1_fs=None, t2_hb=None, t1_hb=None, t2_fs=None,
                  in_fname, out_fname, origin='0 0 40',
                  frame='head', bad=None, autobad='off', skip=None,
                  force=False, st=False, st_buflen=16.0,
                  st_corr=0.96, trans=None, movecomp=False,
                  headpos=False, hp=None, hpistep=None,
                  hpisubt=None, hpicons=True, linefreq=None,
                  cal=None, ctc=None, mx_args='',
                  maxfilter_bin='/neuro/bin/util/maxfilter',
                  logfile=None, n_threads=4):

        """Build a NeuroMag MaxFilter command for later execution.

        See the Maxfilter manual for details on the different options!

        Things to implement
        * check that cal-file matches date in infile!
        * check that maxfilter binary is OK

        Parameters
        ----------
        t1_fs : str
            Input file name
        out_fname : str
            Output file name
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
