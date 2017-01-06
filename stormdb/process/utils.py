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


def convert_dicom_to_nifti(first_dicom, output_fname,
                           converter='mri_convert'):
    with tempfile.TemporaryDirectory() as tmpdir:
        shutil.copytree(os.path.dirname(first_dicom), tmpdir)
        if converter == 'mri_convert':
            cmd = ' '.join(converter, first_dicom, output_fname)
        else:
            raise NotImplementedError('{:s} not known.'.format(converter))

        try:
            subp.check_output([cmd], stderr=subp.STDOUT, shell=False)
        except subp.CalledProcessError as cpe:
            raise RuntimeError('Conversion failed with error message: '
                               '{:s}'.format(cpe.returncode, cpe.output))
