"""
=========================
Utility function for process-modules
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)
import subprocess as subp
import os
import shutil
import tempfile
from glob import glob

from ..base import mkdir_p


def make_copy_of_dicom_dir(dicom_dir, out_dir=None):
    if out_dir is None:
        out_dir = tempfile.mkdtemp()
    else:
        mkdir_p(out_dir)
    for dcm in glob(os.path.join(dicom_dir, '*.*')):
        shutil.copy(dcm, out_dir)
    return out_dir


def first_file_in_dir(input_dir):
    return glob(os.path.join(input_dir, '*.*'))[0]


def convert_dicom_to_nifti(dicom, output_fname,
                           converter='mri_convert'):
    if os.path.isfile(dicom):
        dicom_dir = os.path.dirname(dicom)
    elif os.path.isdir(dicom):
        dicom_dir = dicom

    tmpdir = make_copy_of_dicom_dir(dicom_dir)
    first_dicom = first_file_in_dir(tmpdir)

    if converter == 'mri_convert':
        cmd = ' '.join([converter, first_dicom, output_fname])
    else:
        raise NotImplementedError('{:s} not known.'.format(converter))

    try:
        subp.check_output([cmd], stderr=subp.STDOUT, shell=True)
    except subp.CalledProcessError as cpe:
        raise RuntimeError('Conversion failed with error message: '
                           '{:s}'.format(cpe.returncode, cpe.output))
    finally:
        shutil.rmtree(tmpdir)
