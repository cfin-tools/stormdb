"""
=========================
Helpers for process-tools.
=========================
"""

import os
import errno
import inspect


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
    kwargs = {key: val for key, val in zip(argspec.args[n_pos:],
                                           argspec.defaults)}
    return(args, kwargs)


def mkdir_p(pth):
    """mkdir -p"""
    try:
        os.makedirs(pth)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(pth):
            pass
        else:
            raise


def _get_unique_series(qy, series, subject, modality):
    series = qy.filter_series(description=series, subjects=subject,
                              modalities=modality)
    if len(series) == 0:
        raise RuntimeError('No series found matching {0} for subject '
                           '{1}'.format(series, subject))
    elif len(series) > 1:
        print('Multiple series match the target:')
        print([s['seriename'] for s in series])
        raise RuntimeError('More than one MR series found that '
                           'matches the pattern {0}'.format(series))

    return series
