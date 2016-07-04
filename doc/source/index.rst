.. title:: stormdb-python

stormdb-python
==============

The `stormdb-python` package provides methods to access the StormDB database,
as well as for submitting jobs to be processed on the hyades-cluster.
(:ref:`You can: <what_can_you_do>`):

1. Query the database for a list of series (DICOM sets if MR, raw files if MEG) that match a given string.
2. Submit one or several `maxfilter`-runs to the cluster.

**From raw to filtered MEG data** (:ref:`try it yourself! <getting_started>`):

.. code:: python

    >>> from stormdb.access import Query
    >>> qr = Query(proj_name)


Contents:

.. toctree::
   :maxdepth: 2

   python_reference

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
