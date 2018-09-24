"""
=========================
Helpers for process-tools.
=========================
"""

import os
import errno
import inspect

from six import string_types


def check_destination_writable(dest):
    try:
        open(dest, 'w')
    except IOError:
        return False
    else:
        os.remove(dest)
        return True


def check_source_readable(source):
    try:
        fid = open(source, 'r')
    except IOError:
        return False
    else:
        fid.close()
        return True


def enforce_path_exists(test_dir):
    """Check path exists and is writable"""
    if not os.path.exists(test_dir):
        raise IOError('Non-existent directory: {0}'.format(test_dir))
    if not check_destination_writable(os.path.join(test_dir, 'foo')):
        raise IOError('You do not have write-permission to: '
                      '{0}'.format(test_dir))


def parse_arguments(func):
    # argspec = inspect.getargspec(Raw.filter)
    argspec = inspect.getargspec(func)
    n_pos = len(argspec.args) - len(argspec.defaults)
    # args = argspec.args[1:n_pos]  # drop self
    args = argspec.args[:n_pos]  # drop self
    kwargs = {
        key: val
        for key, val in zip(argspec.args[n_pos:], argspec.defaults)
    }
    return (args, kwargs)


def mkdir_p(pth):
    """mkdir -p"""
    try:
        os.makedirs(pth)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(pth):
            pass
        else:
            raise


def _get_unique_series(qy, series_name, subject, modality):
    series = qy.filter_series(
        description=series_name, subjects=subject, modalities=modality)
    if len(series) == 0:
        raise RuntimeError('No series found matching {0} for subject '
                           '{1}'.format(series_name, subject))
    elif len(series) > 1:
        print('Multiple series match the target:')
        print([s['seriename'] for s in series])
        raise RuntimeError('More than one MR series found that '
                           'matches the pattern {0}'.format(series_name))

    return series


def apply_method(method='recon_all', *args, **kwargs):
    """Apply a method/function with parameters

    Parameters
    ----------
    """
    # cmd = 'self.' + method + "('{}'".format(*args)
    cmd = method + "('{0}'".format(*args)
    for k, v in kwargs.iteritems():
        # if method_args is not None and k in method_args.keys():
        #     v = method_args[k]

        if isinstance(v, string_types):
            cmd += ", {0}='{1}'".format(k, v)
        else:
            cmd += ', {0}={1}'.format(k, v)

    cmd += ')'
    eval(cmd)


def add_to_command(cmd, addition, *args, **kwargs):
    if cmd is None:
        cmd = []

    fmt = addition.format(*args, **kwargs)

    if isinstance(cmd, string_types):
        cmd = '{:s}\n{:s}'.format(cmd, fmt)
    elif isinstance(cmd, list):
        cmd += ['{:s}'.format(fmt)]

    return cmd
