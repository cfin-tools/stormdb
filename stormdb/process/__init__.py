"""Tools for processing data on hyades-cluster."""

# Authors:  Christopher Bailey <cjb@cfin.au.dk>
#           Mads Jensen <mads@cfin.au.dk>
#
# License: BSD (3-clause)

from .maxfilter import Maxfilter
from .mne_python import MNEPython
from .freesurfer import Freesurfer, convert_flash_mris_cfin
from .simnibs import SimNIBS
