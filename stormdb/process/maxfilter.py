"""
=========================
Classes related to Maxfilter

Credits:
    Several functions are modified versions from those in mne-python
    https://github.com/mne-tools/mne-python
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)
import warnings
import numpy as np

from mne.io import Raw
from mne.bem import fit_sphere_to_headshape

from ..base import check_destination_writable, check_source_readable
from ..cluster import ClusterBatch


class Maxfilter(ClusterBatch):
    """ Object for maxfiltering data from database into StormDB filesystem

    Parameters
    ----------
    proj_name : str
        The name of the project.
    bad : list
        List of a priori bad channels (default: empty list)
    verbose : bool
        If True, print out extra information as we go (default: False).

    Attributes
    ----------
    info : dict
        Various info
    """

    def __init__(self, proj_name, bad=[], verbose=False):
        super(Maxfilter, self).__init__(proj_name, verbose=verbose)

        self.info = dict(bad=bad, io_mapping=[])
        # Consider placing other vars here

    def build_cmd(self, in_fname, out_fname, origin='0 0 40',
                  frame='head', bad=None, autobad='off', skip=None,
                  force=False, st=False, st_buflen=16.0,
                  st_corr=0.96, trans=None, movecomp=False,
                  headpos=False, hp=None, hpistep=None,
                  hpisubt=None, hpicons=True, linefreq=None,
                  cal=None, ctc=None, mx_args='',
                  maxfilter_bin='/neuro/bin/util/maxfilter',
                  logfile=None, n_threads=4):

        """Build a NeuroMag MaxFilter command for later execution.

        See the Maxfilter manual for details on the different options!

        Things to implement
        * check that cal-file matches date in infile!
        * check that maxfilter binary is OK

        Parameters
        ----------
        in_fname : str
            Input file name
        out_fname : str
            Output file name
        n_threads : int
            Number of parallel threads to execute on (default: 4)
        maxfilter_bin : str
            Full path to the maxfilter-executable
        logfile : str
            Full path to the output logfile
        force : bool
            Overwrite existing output (default: False)
        origin : array-like or str
            Head origin in mm. If None it will be estimated from headshape
            points.
        frame : str ('device' or 'head')
            Coordinate frame for head center
        bad : str, list (or None)
            List of static bad channels. Can be a list with channel names, or a
            string with channels (with or without the preceding 'MEG')
        autobad : string ('on', 'off', 'n')
            Sets automated bad channel detection on or off
        skip : string or a list of float-tuples (or None)
            Skips raw data sequences, time intervals pairs in sec,
            e.g.: 0 30 120 150
        force : bool
            Ignore program warnings
        st : bool
            Apply the time-domain SSS extension (tSSS)
        st_buflen : float
            tSSS buffer length in sec (disabled if st is False)
        st_corr : float
            tSSS subspace correlation limit (disabled if st is False)
        movecomp : bool (or 'inter')
            Estimates and compensates head movements in continuous raw data.
        trans : str(filename or 'default') (or None)
            Transforms the data into the coil definitions of in_fname,
            or into the default frame. If None, and movecomp is True,
            data will be movement compensated to initial head position.
        headpos : bool
            Estimates and stores head position parameters, but does not
            compensate movements
        hp : string (or None)
            Stores head position data in an ascii file
        hpistep : float (or None)
            Sets head position update interval in ms
        hpisubt : str('amp', 'base', 'off') (or None)
            Subtracts hpi signals: sine amplitudes, amp + baseline, or switch
            off
        hpicons : bool
            Check initial consistency isotrak vs hpifit
        linefreq : int (50, 60) (or None)
            Sets the basic line interference frequency (50 or 60 Hz)
            (None: do not use line filter)
        cal : str
            Path to calibration file
        ctc : str
            Path to Cross-talk compensation file
        mx_args : str
            Additional command line arguments to pass to MaxFilter
        """
        # determine the head origin if necessary
        if origin is None:
            self.logger.info('Estimating head origin from headshape points..')
            raw = Raw(in_fname, preload=False)
            with warnings.filterwarnings('error', category=RuntimeWarning):
                r, o_head, o_dev = fit_sphere_to_headshape(raw.info,
                                                           dig_kind='auto',
                                                           units='m')
            raw.close()

            self.logger.info('Fitted sphere: r = {.1f} mm'.format(r))
            self.logger.info('Origin head coordinates: {.1f} {.1f} {.1f} mm'.
                             format(o_head[0], o_head[1], o_head[2]))
            self.logger.info('Origin device coordinates: {.1f} {.1f} {.1f} mm'.
                             format(o_dev[0], o_dev[1], o_dev[2]))

            self.logger.info('[done]')
            if frame == 'head':
                origin = o_head
            elif frame == 'device':
                origin = o_dev
            else:
                RuntimeError('invalid frame for origin')

        # Start building command
        cmd = (maxfilter_bin + ' -f {:s} -o {:s} -v '.format(in_fname,
                                                             out_fname))

        if isinstance(origin, (np.ndarray, list, tuple)):
            origin = '{:.1f} {:.1f} {:.1f}'.format(origin[0],
                                                   origin[1], origin[2])
        elif not isinstance(origin, str):
            raise(ValueError('origin must be list-like or string'))

        cmd += ' -frame {:s} -origin {:s} -v '.format(frame, origin)

        if bad is not None:
            # format the channels
            if isinstance(bad, str):
                bad = bad.split()
            bad += self.info['bad']  # combine the two
        else:
            bad = self.info['bad']

        # NB add check here that all bads in list actually exist in raw!
        if len(bad) > 0:
            # now assume we have a list of str with channel names
            bad_logic = [ch[3:] if ch.startswith('MEG') else ch for ch in bad]
            bad_str = ' '.join(bad_logic)

            cmd += '-bad {:s} '.format(bad_str)

        cmd += '-autobad {:s} '.format(autobad)

        if skip is not None:
            if isinstance(skip, list):
                skip = ' '.join(['{:.3f} {:.3f}'.format(s[0], s[1])
                                for s in skip])
            cmd += '-skip {:s} '.format(skip)

        if force:
            cmd += '-force '

        if st:
            cmd += '-st '
            cmd += ' {:.0f} '.format(st_buflen)
            cmd += '-corr {:.4f} '.format(st_corr)

        if trans is not None:
            cmd += '-trans {:s} '.format(trans)

        if movecomp:
            cmd += '-movecomp '
            if movecomp == 'inter':
                cmd += ' inter '

        if headpos:
            if movecomp:
                raise RuntimeError('movecomp and headpos mutually exclusive')
            cmd += '-headpos '

        if hp is not None:
            cmd += '-hp {:s} '.format(hp)

        if hpisubt is not None:
            cmd += 'hpisubt {:s} '.format(hpisubt)

        if hpicons:
            cmd += '-hpicons '

        if linefreq is not None:
            cmd += '-linefreq {:d} '.format(linefreq)

        if cal is not None:
            cmd += '-cal {:s} '.format(cal)

        if ctc is not None:
            cmd += '-ctc {:s} '.format(ctc)

        cmd += mx_args

        if logfile:
            cmd += ' | tee ' + logfile

        # NB maxfilter.q hard-coded here, remember to change if cluster changes
        self.add_job(cmd, queue='maxfilter.q', n_threads=n_threads,
                     job_name='maxfilter')
        self.info['io_mapping'] += [dict(input=in_fname, output=out_fname)]

    def print_input_output_mapping(self):
        for io in self.info['io_mapping']:
            print(io['input'])
            print('\t--> {0}'.format(io['output']))

    def check_input_output_mapping(self, force=False):
        for io in self.info['io_mapping']:
            if not check_source_readable(io['input']):
                raise IOError('Input file {0} not '
                              'readable!'.format(io['input']))
            if not check_destination_writable(io['output']):
                if check_source_readable(io['output']) and not force:
                    raise IOError('Output file {0} exists, use force=True to '
                                  'overwrite!'.format(io['output']))
                else:
                    raise IOError('Output file {0} not '
                                  'writable!'.format(io['output']))
        self.logger.info('All inputs readable & outputs writable.')


def Xscan(Maxfilter):
    """Elekta xscan: SSS-based bad channel detection.
    """
    def __init__(self, proj_name, bad=[], verbose=True):
        super(Xscan, self).__init__(proj_name)

        self.info = dict(bad=bad, io_mapping=[])
        print('xscan not implemented yet.')

    # def detect_bad_chans_xscan(self, in_fname, use_tsss=False, n_jobs=1,
    #                            xscan_bin=None, set_bad=True):
    #     """Experimental method from Elekta for detecting bad channels
    #
    #     WARNING! Use at own risk, not documented/fully tested!
    #
    #     Parameters
    #     ----------
    #     in_fname : str
    #         Input file name
    #     use_tsss : bool
    #         If True, uses tSSS-based bad channel estimation (slow!). Default
    #         is False: use tSSS for particularly bad artefacts like dentals.
    #     xscan_bin : str
    #         Full path to xscan-binary (if None, default in /neuro/bin is used)
    #     set_bad : bool
    #         Set the channels found by xscan as bad in the Maxfilter object
    #         (default: True). NB: bad-list is amended, not replaced!
    #     """
    #     _check_n_jobs(n_jobs)
    #
    #     if xscan_bin is None:
    #         xscan_bin = '/neuro/bin/util/xscan'
    #
    #     # Start building command
    #     cmd = [xscan_bin, '-v', '-f', '{:s}'.format(in_fname)]
    #
    #     proc = subp.Popen(cmd, shell=True, stdout=subp.PIPE)
    #     stdout = proc.communicate()[0]  # read stdout
    #     retcode = proc.wait()
    #
    #     if retcode != 0:
    #         if retcode == 127:
    #             raise NameError('xscan binary ' + xscan_bin + ' not found')
    #         else:
    #             errmsg = 'xscan exited with an error, output is:\n\n' + stdout
    #             raise RuntimeError(errmsg)
    #
    #     # CHECKME!
    #     bads_str = []
    #     for il in range(2):
    #         row = stdout[-1*il]
    #         idx = row.find('Static')
    #         if idx > 0 and ('flat' in row or 'bad' in row):
    #             idx = row.find('): ')
    #             bads_str += [row[idx + 3]]
    #
    #     self.logger.info('xscan detected the following bad channels:\n' +
    #                      bads_str)
    #     if set_bad:
    #         new_bads = bads_str.split()
    #         uniq_bads = [b for b in new_bads if b not in self.bad]
    #         self.info['bad'] = uniq_bads
    #         self.logger.info('Maxfilter object bad channel list updated')
