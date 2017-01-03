"""
=========================
Helpers for process-tools.
=========================
"""

import os


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
