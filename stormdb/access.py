"""
=========================
Methods to interact with the STORM database
=========================

"""
# Author: Chris Bailey <cjb@cfin.au.dk>
#
# License: BSD (3-clause)


from getpass import getuser, getpass
from warnings import warn
import os
import requests
from requests import ConnectionError
import re
from six import string_types
from six.moves.urllib.parse import quote_plus
from io import open


class DBError(Exception):
    """
    Exception to raise when StormDB returns an error.
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class Query(object):
    """ Query object for communicating with the STORM database

    Parameters
    ----------
    proj_name : str
        The name of the project.
    stormdblogin : str
        The filename to store database login credentials as a hash.
        The default '~/.stormdblogin' should be OK for everyone. If the file
        does not exist (e.g., for first-time users), the user will be
        prompted for a username and password.
    verbose : bool
        If True, print a lot of messages for debugging. Default: None

    Attributes
    ----------
    proj_name : str
        Name of project
    """

    def __init__(self, proj_name=None, stormdblogin='~/.stormdblogin',
                 verbose=None):
        if proj_name is None:
            try:
                proj_name = os.environ['MINDLABPROJ']
                if proj_name == 'NA' or proj_name == '':
                    raise KeyError('Force a KeyError')
            except KeyError:
                msg = ('You must specify a project name either when creating '
                       'a Query-object, or by setting the MINDLABPROJ '
                       'environment variable (e.g. in your .bashrc file).')
                raise DBError(msg)
        self.proj_name = proj_name  # will be checked later!
        self._stormdblogin = stormdblogin
        self._verbose = verbose

        default_server = 'http://hyades00.pet.auh.dk/modules/StormDb/extract/'
        try_alt_server = False
        try:
            get = requests.get(default_server)
        except ConnectionError:
            try_alt_server = True
        else:
            if not get.status_code == 200:
                try_alt_server = True
            else:
                self._server = default_server

        raise_dberror = False
        if try_alt_server:
            alt_server = 'http://localhost:10080/modules/StormDb/extract/'
            try:
                get = requests.get(alt_server)
            except ConnectionError:
                raise_dberror = True
            else:
                if not get.status_code == 200:
                    raise_dberror = True
                else:
                    self._server = alt_server

        if raise_dberror:
            raise DBError('No access to database server (tried: '
                          '{0} and\n{1})'.format(default_server, alt_server))

        self._get_login_code()
        self._check_login_credentials()  # checks templogin and proj_name

    def _get_login_code(self):
        try:
            with open(os.path.expanduser(self._stormdblogin), 'r') as fid:
                if self._verbose:
                    print('Reading login credentials from ' +
                          self._stormdblogin)
                self._login_code = fid.readline()
        except IOError:
            print('Login credentials not found, please enter them here')
            print('WARNING: This might not work if you\'re in an IDE '
                  '(e.g. spyder)!')

            usr = getuser()

            prompt = 'User \"{:s}\", please enter your password: '.format(usr)
            pwd = getpass(prompt)

            url = 'login/username/' + usr + \
                  '/password/' + quote_plus(pwd)
            output = self._send_request(url, verbose=False)  # never echo pw

            # If we get this far, no DBError was issued above
            print("Code generated, writing to {:s}".format(self._stormdblogin))
            self._login_code = output

            with open(os.path.expanduser(self._stormdblogin), 'wt') as fout:
                # fout.write(self._login_code.encode('UTF-8'))
                fout.write(self._login_code)
            # Use octal representation
            os.chmod(os.path.expanduser(self._stormdblogin), 0o400)

    def _check_response(self, response, error_str='error'):
        if response.find(error_str) != -1:
            if response.find('Your login is not working') != -1:
                # Bad templogin-hash
                msg = 'Looks like your ~/.stormdblogin is old/broken ' +\
                      'and will be removed. Please enter your credentials ' +\
                      'and re-run your query.'
                warn(msg)
                try:
                    os.chmod(os.path.expanduser(self._stormdblogin), 0o600)
                except OSError:  # file doesn't exist
                    pass  # missing ~/.stormdblogin
                else:
                    os.remove(os.path.expanduser(self._stormdblogin))
                self._get_login_code()

            else:
                if response.find('Could not login') != -1:
                    msg = ('Invalid username or wrong password!')
                elif response.find('The project does not exist') != -1:
                    msg = ('The project ID/code used ({0}) '
                           'does not exist in the database, please '
                           'check.'.format(self.proj_name))
                else:
                    msg = ('StormDB reports error "{0}", not sure what to do '
                           'about it.'.format(response))
                raise DBError(msg)
        elif response.find("<!DOCTYPE html>") == 0:
            msg = ('Poorly formed HTTP GET string, this should not happen! '
                   'Contact a member of the server administration.')
            raise DBError(msg)

    def _check_login_credentials(self):
        '''Check that a valid stormdblogin and project name are given.
        '''
        url = ('testlogin?{login:s}&projectCode={proj:s}'
               ''.format(login=self._login_code, proj=self.proj_name))
        self._send_request(url)

    def _send_request(self, url, verbose=None):
        # This rather strange logic enables the following. Note that this
        # method is private, meaning we control the call logic tightly
        # - the "global" (instance-level) verbosity will be effective unless
        #   the method is called with an explicit verbosity
        # - an explicit False will silence a globally True verbosity!
        if verbose is True or (verbose is None and self._verbose):
            this_verbose = True
        else:
            this_verbose = False

        full_url = self._server + url
        if this_verbose:
            print(full_url)

        try:
            req = requests.get(full_url)
        except:
            print('hyades00 is not responding, it may be down.')
            print('Contact a system administrator for confirmation.')
            raise

        response = req.content.decode(encoding='UTF-8')
        if this_verbose:
            print(response)

        try:
            self._check_response(response)
        except DBError as e:
            if this_verbose:
                print(10 * '*' + ' Last GET string: ' + 10 * '*')
                print(full_url)
                print(38 * '*')
            raise e

        # Python 3.x treats pipe strings as bytes, which need to be encoded
        # Here assuming shell output is in UTF-8
        return(response)

    def get_subjects(self, subj_type='included',
                     has_modality=None, has_series=None):
        """Get list of subjects from database

        Parameters
        ----------
        subj_type : str
            Can be 'included' (default), 'excluded' or 'all'.
        has_modality : str | None
            Optionally only return subjects that have a specified modality.
        has_series : str | None
            Optionally only return subjects that have a specified series.

        Returns
        -------
        subjects : list of str
            Subject ID codes as returned by the database.
            If no subjects are found, an empty list is returned
        """
        if has_modality is not None and has_series is not None:
            raise ValueError(
                'You can only specify a modality OR a series, not both.')

        # using 'subjecs' here would return only numeric ID, not code
        scode = 'subjectswithcode'
        if subj_type == 'included':
            included = 1
        elif subj_type == 'excluded':
            included = -1
        elif subj_type == 'all':
            included = 0
        else:
            raise NameError("subj_type must be 'included', excluded' or 'all'")

        url = ('{scode:s}?{login:s}&projectCode={proj:s}&included={incl:d}'
               .format(scode=scode, login=self._login_code,
                       proj=self.proj_name, incl=included))
        output = self._send_request(url)

        # Split at '\n'
        subj_list = output.split('\n')
        # Remove any empty entries!
        subj_list = [x for x in subj_list if x]

        if has_modality is not None:
            all_series = self.filter_series(modalities=has_modality)
            # get unique subjects that have has_modality and were found above
            used = []
            subj_list = [ser['subjectcode'] for ser in all_series if
                         ser['subjectcode'] not in used and
                         (used.append(ser['subjectcode']) or True) and
                         ser['subjectcode'] in subj_list]
            # The following also works, but is slower?
            # pop_inds = []
            # for nsub, sub in enumerate(subj_list):
            #     studs = self.get_studies(sub, modality=has_modality)
            #     if len(studs) == 0:
            #         pop_inds.append(nsub)
            # pop_inds.reverse()  # prune from back
            # for pi in pop_inds:
            #     subj_list.pop(pi)

        if has_series is not None:
            all_series = self.filter_series(description=has_series)
            used = []
            subj_list = [ser['subjectcode'] for ser in all_series if
                         ser['subjectcode'] not in used and
                         (used.append(ser['subjectcode']) or True) and
                         ser['subjectcode'] in subj_list]

        return(subj_list)

    def get_studies(self, subj_id, modality=None, unique=False):
        """Get list of studies from database for specified subject

        Parameters
        ----------
        subj_id : str
            A string uniquely identifying a subject in the database.
            For example: '0001_ABC'
        modality : str | None
            A string defining the modality of the studies to get. Valid
            examples include 'MEG' and 'MR'. If None, all studies are
            returned regardless of modality.
        unique : bool
            If True, only the chronologically first study of the desired
            modality is returned. Default is False (return all studies
            that match the modality).

        Returns
        -------
        studies : list of str
            Study IDs as returned by the database.
            If no studies are found, an empty list is returned
        """

        url = 'studies?' + self._login_code + \
            '&projectCode=' + self.proj_name + '&subjectNo=' + subj_id
        output = self._send_request(url)

        # Split at '\n'
        stud_list = output.split('\n')
        # Remove any empty entries!
        stud_list = [x for x in stud_list if x]

        if modality:
            for ii, study in enumerate(stud_list):
                url = 'modalities?' + self._login_code + \
                    '&projectCode=' + self.proj_name + '&subjectNo=' + \
                      subj_id + '&study=' + study
                output = self._send_request(url).split('\n')

                if modality in output:
                    if unique:
                        return([study, ])  # always return a list
                else:
                    stud_list[ii] = None

            # In Py3, filter returns an iterable object, but here we want list
            stud_list = list(filter(None, stud_list))

        return(stud_list)

    def get_series(self, subj_id, study, modality):
        """Get dict of series from database.

        Specify subject, study and modality. The database series numbers are
        keys, names are values.

        Parameters
        ----------
        subj_id : str
            A string uniquely identifying a subject in the database.
            For example: '0001_ABC'
        study : str
            A string uniquely identifying a study in the database for
            given subject.
        modality : str
            A string defining the modality of the study to get.

        Returns
        -------
        series : dict
            A dictionary with keys corresponding to the series names, as
            defined in the database, and values corresponding to the
            index of the series in the study (1-based). If no series are
            found, an empty dict is returned.

        Notes
        -----
        The choice of a dict as output can be reconsidered.
        """
        url = 'series?' + self._login_code + '&projectCode=' + \
            self.proj_name + '&subjectNo=' + \
            subj_id + '&study=' + study + '&modality=' + modality
        output = self._send_request(url)

        # Split at '\n'
        series_list = output.split('\n')
        # Remove any empty entries!
        series_list = [x for x in series_list if x]

        # create a 2D list with series name (as string)
        # in 1st column and numerical index (also as string) in 2nd column
        series_list_2d = [x.split(' ') for x in series_list]

        series_dict = {key: value for key, value in series_list_2d}

        return(series_dict)

    def get_files(self, subj_id, study, modality, series):
        """Get list of files from database for specified subject, study,
        modality and series.

        Parameters
        ----------
        subj_id : str
            A string uniquely identifying a subject in the database.
            For example: '0001_ABC'
        study : str
            A string uniquely identifying a study in the database for
            given subject.
        modality : str
            A string defining the modality of the study to get.
        series : str or int
            A string or int defining the index (1-based) of the series to get.

        Returns
        -------
        files : list of str
            List of absolute pathnames to file(s) in series. If no files are
            found, an empty list is returned.
        """
        if type(series) is int:
            series = str(series)

        url = 'files?' + self._login_code + '&projectCode=' + \
              self.proj_name + '&subjectNo=' + subj_id + '&study=' + \
              study + '&modality=' + modality + '&serieNo=' + series
        output = self._send_request(url)

        # Split at '\n'
        file_list = output.split('\n')
        # Remove any empty entries!
        file_list = [x for x in file_list if x]

        return(file_list)

    def filter_series(self, description=None, subjects=None, modalities=None,
                      study_metas=None, return_files=True):
        """Select series based on their description (name)

        Get list of series (and corresponding files) from database matching
        a description. Queries may be performed on specific subject(s) and
        modalities. In addition, study-level meta-information can be used.

        Parameters
        ----------
        description : str | list of str | None
            A string or list of strings containing the name of the series to
            extract. The asterisk ('*') may be used as a wildcard. None
            (default) will match any series description.
        subjects : str | list of str | None
            A string or list or strings identifying one or more subject IDs in
            the database. For example: ['0001_ABC', '0010_XYZ']. None (default)
            is equivalent to all non-excluded subjects.
        modalities : str | list of str | None
            A string or list or strings identifying one or more modalities of
            the study to get. None is equivalent to all modalities.
        study_metas : dict or None
            A dictionary with fields "name", "comparison" and "value", e.g.,
            dict(name='timepoint', comparison='=', value=2). By default all
            studies are returned.
        return_files : bool
            Default is True: return the names of the files for each series.

        Returns
        -------
        info_dict_list : list of dict
            List of dictionaries containing information for each series
            matching the filter settings. The important keys are:

            subjectcode : str
                The database subject code in the form NNNN_XYZ
            path : str
                path to files
            seriename : str
                name of the series (corresponding to the name of the original
                data file in MEG/EEG)
            files : list of str
                list of strings with file names
        """
        types = ''  # return all types of series (DICOM)
        anywithtype = '0'  # even return series without a type
        excluded = '0'
        studies = ''  # for study-filtering, use either metas or write new func
        meta_str = ''
        outp = ''
        removeProjects = ''

        if isinstance(description, list):
            try:
                description_str = '|'.join(description)
            except TypeError:
                raise DBError('When using a list of descriptions, each '
                              'element must be a string.')
        elif description is None:
            description_str = ''
        elif not isinstance(description, string_types):
            raise DBError('description-parameter must be str or list of str')
        else:
            description_str = description

        if isinstance(subjects, list):
            try:
                subjects_str = '|'.join(subjects)
            except TypeError:
                raise DBError('When using a list of subjects, each element '
                              'must be a string.')
        elif subjects is None:
            subjects_str = ''
        elif not isinstance(subjects, string_types):
            raise DBError('subjects-parameter must be str or list of str')
        else:
            subjects_str = subjects

        if isinstance(modalities, list):
            try:
                modalities_str = '|'.join(modalities)
            except TypeError:
                raise DBError('When using a list of modalities, each element '
                              'must be a string.')
        elif modalities is None:
            modalities_str = ''
        elif not isinstance(modalities, string_types):
            raise DBError('modalities-parameter must be str or list of str')
        else:
            modalities_str = modalities

        if study_metas is not None and isinstance(study_metas, dict):
            # do some checking here...
            try:
                meta_str += 'studymetas[{:s}]={:s}${:d}&'.\
                    format(study_metas['name'],
                           study_metas['comparison'],
                           study_metas['value'])
            except:
                print('Problem with study_metas:')
                print(study_metas)
                raise

        if return_files:
            # do some checking here...
            outp += 'outputoptions[inclfiles]=1&'

        url = 'filteredseries?' + self._login_code + '&projectCode=' + \
              self.proj_name + '&subjects=' + subjects_str + '&studies=' + \
              studies + '&modalities=' + modalities_str + \
              '&types=' + types + '&anyWithType=' + anywithtype + \
              '&description=' + description_str + '&excluded=' + excluded +\
              '&' + meta_str + outp + '&removeProjects=' + removeProjects
        output = self._send_request(url)

        # Split at '\n'
        info_list = output.split('\n')
        # Remove any empty entries!
        info_list = [x for x in info_list if x]

        info_dict_list = []
        for il in info_list:
            info = []  # for each matched series, prepare a new dict
            for kvp in il.split('$'):
                key_val_pair = kvp.split(':')
                if 'files' in key_val_pair[0]:
                    key_val_pair[1] = key_val_pair[1].split('|')
                elif 'path' in key_val_pair[0]:
                    m = re.search('\d{3}\.(.+?)/files',
                                  key_val_pair[1])
                    info.append(['seriename', m.group(1)])
                info.append(key_val_pair)
            info_dict = {key: value for (key, value) in info}
            info_dict_list.append(info_dict)

        return(info_dict_list)

    # def generate_output_path(self, relative_path=None):
    #     full_path = opj(self._scratch, relative_path)

if __name__ == '__main__':

    project_code = 'MEG_service'

    Q = Query(proj_name=project_code)
    print(Q)
