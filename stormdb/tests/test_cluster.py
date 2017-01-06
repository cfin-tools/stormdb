from stormdb.cluster import Cluster, ClusterJob, ClusterBatch
from nose.tools import assert_true, assert_equal, assert_raises


test_cmd = 'sleep 5'
proj_name = 'MEG_EEG-Training'
test_subject_name = '0002_M55'
test_queue = 'short.q'
highmem_queue = 'highmem.q'
working_dir = '/tmp'  # assume this is always present


def test_job_exceptions():
    assert_raises(ValueError, ClusterJob)
    assert_raises(ValueError, ClusterJob, cmd=test_cmd, proj_name=None)
    assert_raises(ValueError, ClusterJob, cmd=test_cmd, proj_name=proj_name,
                  queue='nonexistent-queue')
    assert_raises(RuntimeError, ClusterJob, cmd=test_cmd, proj_name=proj_name,
                  queue=highmem_queue, h_vmem=None)  # mem usage?!


def test_job_working_dir():
    assert_raises(IOError, ClusterJob, cmd=test_cmd, proj_name=proj_name,
                  queue=test_queue, working_dir='/non/existent/dir')
    assert_raises(IOError, ClusterJob, cmd=test_cmd, proj_name=proj_name,
                  queue=test_queue, working_dir='/')  # no write permission
    job = ClusterJob(cmd=test_cmd, proj_name=proj_name,
                     queue=test_queue, working_dir='cwd')
    assert_true('$ -cwd\n' in job._qsub_script)
    job = ClusterJob(cmd=test_cmd, proj_name=proj_name,
                     queue=test_queue, working_dir=working_dir)
    assert_true('$ -wd {:s}\n'.format(working_dir) in job._qsub_script)

    job.submit()
