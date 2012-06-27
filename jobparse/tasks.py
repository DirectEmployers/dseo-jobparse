import os
import sys

from celery.task import task

import import_jobs

@task(name="tasks.task_refresh_bunit_jobs")
def task_refresh_bunit_jobs(jsid, **kwargs):
    import_jobs.refresh_bunit_jobs(jsid, **kwargs)

@task(name="tasks.task_update_solr")
def task_update_solr(jsid, **kwargs):
    import_jobs.update_solr(jsid, **kwargs)

@task(name="tasks.task_clear_solr")
def task_clear_solr(jsid):
    """Delete all jobs for a given Business Unit/Job Source."""
    import_jobs.clear_solr(jsid)

@task(name="tasks.task_clear")
def task_clear(jsid):
    import_jobs.clear_jobs(jsid.id)

