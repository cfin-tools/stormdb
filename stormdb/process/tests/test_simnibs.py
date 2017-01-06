import os
from stormdb.process import SimNIBS
from nose.tools import assert_true, assert_equal, assert_raises


proj_name = 'MEG_EEG-Training'
test_subject_name = '0002_M55'
t1_fs = 't1_mpr_sag_weakFS'
t2_hb = 't2_tse_sag_HighBW'
t1_hb = 't1_mpr_sag_HighBW'
t2_fs = 't2_tse_sag_FS'
output_dir = 'scratch/sn_subjects_dir'


def test_init():
    if 'SN_SUBJECTS_DIR' not in os.environ.keys():
        assert_raises(ValueError, SimNIBS, proj_name)  # define outdir
    assert_raises(IOError, SimNIBS, proj_name, output_dir='')
