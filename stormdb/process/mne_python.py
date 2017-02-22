import os
from .utils import (_get_absolute_proj_path)
from ..base import (enforce_path_exists, check_destination_writable,
                    check_source_readable)
from ..cluster import ClusterBatch


class MNEPython(ClusterBatch):
    """Clusterised mne-python commands.
    """
    def __init__(self, proj_name, bad=[], verbose=True):
        super(MNEPython, self).__init__(proj_name, verbose=verbose)

        self.info = dict(bad=bad, io_mapping=[])

    def raw_filter(self, in_fname, out_fname, l_freq, h_freq, **kwargs):
        if not check_source_readable(in_fname):
            raise IOError('Input file {0} not readable!'.format(in_fname))
        if not check_destination_writable(out_fname):
            raise IOError('Output file {0} not writable!'.format(out_fname))

        script = ("from mne.io import read_raw_fif;"
                  "raw = read_raw_fif('{in_fname:s}', preload=True);"
                  "raw.filter({l_freq}, {h_freq}{kwargs:});"
                  "raw.save('{out_fname:s}')")
        filtargs = ', '.join("{!s}={!r}".format(key, val) for
                             (key, val) in kwargs.items())
        filtargs = ', ' + filtargs if len(kwargs) > 0 else filtargs
        cmd = "python -c \""
        cmd += script.format(in_fname=in_fname, out_fname=out_fname,
                             l_freq=l_freq, h_freq=h_freq, kwargs=filtargs)
        cmd += "\""

        self.add_job(cmd, n_threads=1, job_name='mne.raw.filter')
        self.info['io_mapping'] += [dict(input=in_fname, output=out_fname)]

    def setup_source_space(self, subject, src_fname, **kwargs):

        subjects_dir = self._triage_subjects_dir_from_kwargs(kwargs)

        enforce_path_exists(os.path.join(subjects_dir, subject))
        if not check_destination_writable(src_fname):
            raise IOError('Output file {0} not writable!'.format(src_fname))

        script = ("from mne import setup_source_space;"
                  "setup_source_space('{subject:s}', fname='{src_fname:s}'"
                  "{kwargs:});")
        filtargs = ', '.join("{!s}={!r}".format(key, val) for
                             (key, val) in kwargs.items())
        filtargs = ', ' + filtargs if len(kwargs) > 0 else filtargs
        cmd = "python -c \""
        cmd += script.format(subject=subject, src_fname=src_fname,
                             kwargs=filtargs)
        cmd += "\""

        self.add_job(cmd, n_threads=1, job_name='mne.src_space')
        self.info['io_mapping'] += [dict(input=subject, output=src_fname)]

    def prepare_bem_model(self, subject, bem_fname, **kwargs):
        subjects_dir = self._triage_subjects_dir_from_kwargs(kwargs)
        enforce_path_exists(os.path.join(subjects_dir, subject))
        if not check_destination_writable(bem_fname):
            raise IOError('Output file {0} not writable!'.format(bem_fname))

        script = ("from mne import (make_bem_model, make_bem_solution, "
                  "write_bem_solution);\n"
                  "surfs = make_bem_model('{subject:s}'{kwargs:});\n"
                  "bem = make_bem_solution(surfs);\n"
                  "write_bem_solution('{bem_fname:s}', bem)\n")
        filtargs = ', '.join("{!s}={!r}".format(key, val) for
                             (key, val) in kwargs.items())
        filtargs = ', ' + filtargs if len(kwargs) > 0 else filtargs
        cmd = "python -c \""
        cmd += script.format(subject=subject, bem_fname=bem_fname,
                             kwargs=filtargs)
        cmd += "\""

        self.add_job(cmd, n_threads=1, job_name='mne.prep_bem')
        self.info['io_mapping'] += [dict(input=subject, output=bem_fname)]

    def make_forward_solution(self, meas_fname, trans_fname, bem_fname,
                              fwd_fname, **kwargs):
        for fname in (meas_fname, trans_fname, bem_fname):
            if not check_source_readable(fname):
                raise IOError('Input file {} not readable!'.format(fname))
        if not check_destination_writable(fwd_fname):
            raise IOError('Output file {} not writable!'.format(bem_fname))

        script = ("from mne import make_forward_solution;\n"
                  "make_forward_solution('{meas:s}', '{trans:s}', "
                  "'{bem:s}', fname='{fwd:s}'{kwargs:});\n")
        filtargs = ', '.join("{!s}={!r}".format(key, val) for
                             (key, val) in kwargs.items())
        filtargs = ', ' + filtargs if len(kwargs) > 0 else filtargs
        cmd = "python -c \""
        cmd += script.format(meas=meas_fname, trans=trans_fname, bem=bem_fname,
                             fwd=fwd_fname, kwargs=filtargs)
        cmd += "\""

        self.add_job(cmd, n_threads=1, job_name='mne.fwd_solve')
        self.info['io_mapping'] += [dict(input=meas_fname, output=fwd_fname)]

    def _triage_subjects_dir_from_kwargs(self, kwargs):
        if 'subjects_dir' not in kwargs.keys():
            if 'SUBJECTS_DIR' in os.environ.keys():
                subjects_dir = os.environ['SUBJECTS_DIR']
            else:
                raise ValueError('No SUBJECTS_DIR defined! You must do so '
                                 'either by using an argument to this method, '
                                 'or by setting the SUBJECT_DIR environment '
                                 'variable. The directory must exist.')
        else:
            subjects_dir = _get_absolute_proj_path(kwargs['subjects_dir'],
                                                   self.proj_name)
            os.environ['SUBJECTS_DIR'] = subjects_dir
