"""
=========================
Helpers for process-tools.
=========================
"""

import os
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
