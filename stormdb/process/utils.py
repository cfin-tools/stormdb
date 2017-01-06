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


def convert_dicom_to_nifti(first_dicom, output_fname,
                           converter='mri_convert'):
    tmpdir = tempfile.mkdtemp()
    dicom_dir = os.path.dirname(first_dicom)
    first_dicom = os.path.join(tmpdir, os.path.basename(first_dicom))
    for dcm in glob(os.path.join(dicom_dir, '*.*')):
        shutil.copy(dcm, tmpdir)

    if converter == 'mri_convert':
        cmd = ' '.join([converter, first_dicom, output_fname])
    else:
        raise NotImplementedError('{:s} not known.'.format(converter))

    try:
        subp.check_output([cmd], stderr=subp.STDOUT, shell=True)
    except subp.CalledProcessError as cpe:
        raise RuntimeError('Conversion failed with error message: '
                           '{:s}'.format(cpe.returncode, cpe.output))
    shutil.rmtree(tmpdir)
