import inspect

from .base import check_destination_writable, check_source_readable
from ..cluster import ClusterBatch


class MNEPython(ClusterBatch):
    """Clusterised mne-python commands.
    """
    def __init__(self, proj_name, bad=[], verbose=True):
        super(MNEPython, self).__init__(proj_name)

        self.info = dict(bad=bad, io_mapping=[])

    def parse_arguments(self, func):
        # argspec = inspect.getargspec(Raw.filter)
        argspec = inspect.getargspec(func)
        n_pos = len(argspec.args) - len(argspec.defaults)
        args = argspec.args[1:n_pos]  # drop self
        kwargs = {key: val for key, val in zip(argspec.args[n_pos:],
                                               argspec.defaults)}
        return(args, kwargs)

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
