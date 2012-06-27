import os
import sys
import urllib
import datetime
import logging
from itertools import izip_longest

from lxml import etree
from pysolr import Solr
    
from django.conf import settings
from django.db import transaction

import xmlparse
from .helpers import slices
from .models import BusinessUnit, jobListing

BASE_DIR = settings.BASE_DIR
DATA_DIR = settings.DATA_DIR
FEED_FILE_PREFIX = "dseo_feed_"

def refresh_bunit_jobs(buid, download=True, update_all=True):
    """
    Writes new and/or updated job data for a particular Business Unit to
    the RDBMS.

    Inputs:
    :buid: A numeric string, e.g. "13", "283". Corresponds to a given
    BusinessUnit id.
    :update_all: Boolean. If 'True', all jobs in the feed file will be
    sent to the database to be updated.

    Returns:
    None

    Writes/Modifies:
    Job data as provided by `import_jobs.parse_feed_file` is used to
    modify the RDBMS. This includes UPDATE, INSERT and DELETE operations.
    
    """
    logging.info("XML Jobs Feed - Refresh for Buid: %s" % buid)
    
    if download:
        filepath = download_feed_file(buid)
    else:
        filepath = os.path.join(DATA_DIR, FEED_FILE_PREFIX + str(buid) +
                                '.xml')
    results = {}
    changes = False
    if update_all:
        results = parse_feed_file(filepath, buid, update_all)
        # UIDs of jobs in the feed file but not in the database.
        newjobs = results['jobs_to_save']
        # UIDs of jobs in the database but not in the feed file.
        jobs_to_delete = results['deleted_jobs_ids']
        num_old_jobs = len(jobs_to_delete)
        changes = bool(newjobs or jobs_to_delete)

        if changes:
            if newjobs:
                logging.info("BUID:%s - DB - Updating %s jobs" %
                             (buid, len(newjobs)))
                save_jobs(results['jobs_to_save'])

            if num_old_jobs:
                logging.info("BUID:%s - DB - Deleting %s jobs" %
                             (buid, num_old_jobs))
                _remove_old_jobs(jobs_to_delete)

    _update_business_unit_modified_dates(buid, results.get('crawled_date'),
                                         updated=changes)
            
    logging.info("Import complete for buid: %s" % buid)

def _remove_old_jobs(jobs):
    errors = []
    try:
        jobListing.objects.filter(uid__in=list(jobs)).delete()
    except Exception, e:
        errors.append(e)

    return errors
    
def parse_feed_file(filepath, buid, update_all_jobs=True):
    """
    Leverage the `xmlparse' module to calculate which jobs to add, delete
    and/or update in the database.

    Input:
    :buid: An integer or string. Corresponds to the id of a particular
    Business Unit.
    :update_all_jobs: Boolean. If 'True', all jobs in the feed file will be
    sent to the database to be updated.

    Returns:
    :output: A dictionary.

    """
    jobfeed = xmlparse.DEv2JobFeed(filepath)
    output = {
        # jobListing instances whose UID is in new_jobs_ids
        'jobs_to_save': [], 
        # The jobs in the feed file, but not in the database. These jobs
        # need to be added to the database and to Solr.
        'new_jobs_ids': set(),
        # Jobs in the database, but not in the feed file. These jobs need
        # to be removed from the database and from Solr.
        'deleted_jobs_ids': set(),
        # The jobs in the database right now
        'current_jobs': set(),
        'crawled_date': jobfeed.crawled_date,
        'errors': _xml_errors(jobfeed)
    }

    # If the feed file did not pass validation, return.
    if jobfeed.errors:
        error = jobfeed.error_messages
        logging.error("BUID:%s - Feed file has failed validation on line %s. "
                      "Exception: %s" % (error['buid'], error['line'],
                                         error['exception']))
        return output

    jobs = jobfeed.joblist()
    output['current_jobs'] = set(
        jobListing.objects.filter(buid=buid).values_list('uid', flat=True)
    )
    job_uids = set([long(i.uid) for i in jobs if i.uid])
    current_jobs = output['current_jobs']
    output['deleted_jobs_ids'] = current_jobs.difference(job_uids)

    # If update_all_jobs is False, calculate the jobs that are in the feed
    # file but not in the database. Effectively, this results in an "append-
    # only" operation, in that only new rows are being added; existing rows
    # will not be updated with any new information.
    #
    # Alternatively, if update_all_jobs is True, every job instance created
    # by jobfeed.joblist() will be saved to the database, updating all
    # existing rows and adding new ones.
    if not update_all_jobs:
        output['new_jobs_ids'] = job_uids.difference(current_jobs)
        output['jobs_to_save'] = filter(lambda x: _job_filter(x) in
                                        output['new_jobs_ids'], jobs)
    else:
        output['new_jobs_ids'] = job_uids
        output['jobs_to_save'] = jobs

    logging.info("XML Job Feed Processed for Buid: %s" % buid,
                 extra={
                     "data": {
                         "number of jobs": len(output['jobs_to_save']),
                         "date/time": datetime.datetime.utcnow()
                     }
                 })
    os.remove(filepath)
    logging.info("BUID:%s - Deleted feed file." % buid)
    return output

def update_solr(buid, download=True, force=True, set_title=False):
    """
    Update the Solr master index with the data contained in a feed file
    for a given buid/jsid.

    This is meant to be a standalone function such that the state of the
    Solr index is not tied to the state of the database.

    Inputs:
    :buid: An integer; the ID for a particular business unit.
    :download: Boolean. If False, this process will not download a new
    feedfile, but instead use the one on disk. Should only be false for
    the purposes of our test suite.
    :force: Boolean. If True, every job seen in the feed file will be
    updated in the index. Otherwise, only the jobs seen in the feed file
    but not seen in the index will be updated. This latter option will
    soon be deprecated.

    Returns:
    A 2-tuple consisting of the number of jobs added and the number deleted.

    Writes/Modifies:
    Job data found in the feed file is used to modify the Solr index. This
    includes adds & deletes. (Solr does not have a discrete equivalent to
    SQL's UPDATE; by adding a document with the same UID as a document in
    the index, the equivalent of an update operation is performed.)

    """
    if download:
        filepath = download_feed_file(buid)
    else:
        filepath = os.path.join(DATA_DIR, FEED_FILE_PREFIX + str(buid) +
                                '.xml')
    jobfeed = xmlparse.DEv2JobFeed(filepath)

    # If the feed file did not pass validation, return. The return value is
    # '(0, 0)' to match what's returned on a successful parse.
    if jobfeed.errors:
        error = jobfeed.error_messages
        logging.error("BUID:%s - Feed file has failed validation on line %s. "
                      "Exception: %s" % (error['buid'], error['line'],
                                         error['exception']))
        return (0, 0)
        
    bu = BusinessUnit.objects.get(id=buid)

    # 'set_title' will be True if this feed file is for a BusinessUnit that's
    # been newly created by `helpers.create_businessunit` (called from the
    # `send_sns_confirm` view).
    if set_title or not bu.title:
        bu.title = jobfeed.company
        bu.save()

    # A list of jobListing instances based off the job data in the feed file
    # for the business unit.
    jobs = jobfeed.jobparse()
    # Build a set of all the UIDs for all those instances.
    job_uids = set([long(i.get('uid')) for i in jobs if i.get('uid')])
    conn = Solr(settings.HAYSTACK_CONNECTIONS['default']['URL'])
    step1 = 1024

    # Get the count of all the results in the Solr index for this BUID.
    hits = conn.search("*:*", fq="buid:%s" % buid, facet="false",
                       mlt="false").hits
    # Create (start-index, stop-index) tuples to facilitate handling results
    # in ``step1``-sized chunks. So if ``hits`` returns 2048 results,
    # ``job_slices`` will look like ``[(0,1024), (1024, 2048)]``. Those
    # values are then used to slice up the total results.
    #
    # This was put in place because part of the logic to figuring out what
    # jobs to delete from and add jobs to the Solr index is using set
    # algebra. We convert the total list of UIDs in the index and the UIDs
    # in the XML feed to sets, then compare them via ``.difference()``
    # (seen below). However for very large feed files, say 10,000+ jobs,
    # this process was taking so long that the connection would time out. To
    # address this problem we break up the comparisons as described above.
    # This results in more requests but it alleviates the connection timeout
    # issue.
    job_slices = slices(range(hits), step=step1)
    results = [_solr_results_chunk(tup, buid, step1) for tup in job_slices]
    solr_uids = reduce(lambda x, y: x|y, results) if results else set()
    # Return the job UIDs that are in the Solr index but not in the feed
    # file.
    solr_del_uids = solr_uids.difference(job_uids)

    if not force:
        # Return the job UIDs that are in the feed file but not in the Solr
        # index.
        solr_add_uids = job_uids.difference(solr_uids)
        # ``jobfeed.solr_jobs()`` yields a list of dictionaries. We want to
        # filter out any dictionaries whose "uid" key is not in
        # ``solr_add_uids``. This is because by default we only want to add
        # new documents (which each ``solr_jobs()`` dictionary represents),
        # not update.
        add_docs = filter(lambda x: int(x.get("uid", 0)) in solr_add_uids,
                          jobfeed.solr_jobs())
    else:
        # This might seem redundant to refer to the same value
        # twice with two different variable names. However, this decision
        # was made during the implementation of the "force Solr update"
        # feature to this function.
        #
        # Instead of adding only the documents with UIDs that are in the feed
        # file but not in the Solr index, we're going to add ALL the documents
        # in the feed file. This will add the new documents of course, but it
        # will also update existing documents with any new data. Uniqueness of
        # the documents is ensured by the ``id`` field defined in the Solr
        # schema (the template for which can be seen in
        # templates/search_configuration/solr.xml). At the very bottom you'll
        # see <uniqueKey>id</uniqueKey>. This serves as the equivalent of the pk
        # (i.e. globally unique) in a database.
        solr_add_uids = job_uids
        add_docs = jobfeed.solr_jobs()
        
    # Slice up ``add_docs`` in chunks of 4096. This is because the
    # maxBooleanClauses setting in solrconfig.xml is set to 4096. This means
    # if we used any more than that Solr would throw an error and our
    # updates wouldn't get processed.
    add_steps = slices(range(len(solr_add_uids)), step=4096)
    # Same concept as ``add_docs``.
    del_steps = slices(range(len(solr_del_uids)), step=4096)
    # Create a generator that yields 2-tuples with each invocation. The
    # 2-tuples consist of one tuple each from del_steps & add_steps. Any
    # mismatched values (e.g. there are more del_steps than add_steps)
    # will be compensated for with the ``fillvalue``.
    zipped_steps = izip_longest(del_steps, add_steps, fillvalue=(0,0))
    
    for tup in zipped_steps:
        update_chunk = add_docs[tup[1][0]:tup[1][1]+1]

        if update_chunk:
            logging.info("BUID:%s - SOLR - Update chunk: %s" %
                         (buid, [i['uid'] for i in update_chunk]))
            # Pass 'commitWithin' so that Solr doesn't try to commit the new
            # docs right away. This will help relieve some of the resource
            # stress during the daily update. The value is expressed in
            # milliseconds.
            conn.add(update_chunk, commitWithin="30000")

        delete_chunk = _build_solr_delete_query(list(solr_del_uids)[tup[0][0]:\
                                                                    tup[0][1]+1])

        if delete_chunk:
            logging.info("BUID:%s - SOLR - Delete chunk: %s" %
                         (buid, list(solr_del_uids)))
            conn.delete(q=delete_chunk)

    os.remove(filepath)
    logging.info("BUID:%s - Deleted feed file." % buid)
    return len(solr_add_uids), len(solr_del_uids)

def clear_solr(buid):
    """Delete all jobs for a given business unit/job source."""
    conn = Solr(settings.HAYSTACK_CONNECTIONS['default']['URL'])
    hits = conn.search(q="*:*", rows=1, mlt="false", facet="false").hits
    logging.info("BUID:%s - SOLR - Deleting all %s jobs" % (buid, hits))
    conn.delete(q="buid:%s" % buid)
    logging.info("BUID:%s - SOLR - All jobs deleted." % buid)

def _solr_results_chunk(tup, buid, step):
    """
    Takes a (start_index, stop_index) tuple and gets the results in that
    range from the Solr index.

    """
    conn = Solr(settings.HAYSTACK_CONNECTIONS['default']['URL'])
    results = conn.search("*:*", fq="buid:%s" % buid, fl="uid",
                          rows=step, start=tup[0], facet="false",
                          mlt="false").docs
    return set([i['uid'] for i in results])
    
def _job_filter(job):
    if job.uid:
        return long(job.uid)

def _xml_errors(jobfeed):
    """
    Checks XML input for errors, and logs any it finds.

    """
    if jobfeed.errors:
        logging.error("XML Job Feed Error",
                      extra={'data': jobfeed.error_messages})
    return jobfeed.error_messages

@transaction.commit_manually
def save_jobs(jobs):
    """
    Process a list of dictionaries, each describing the attributes of a
    single jobListing instance.

    Input:
    :jobs: A list of unsaved jobListing instances.

    Returns:
    :saved_jobs: A list of jobListing instances.

    Writes/Modifies:
    Rows on the seo_joblisting table that correspond to the jobs being
    modified.

    """
    saved_jobs = []

    for job in jobs:
        try:
            target = jobListing.objects.get(uid=job.uid)
            job.id = target.id
        except jobListing.DoesNotExist:
            pass

        try:
            job.save()
        except Exception as e:
            logging.info(e)
            logging.info(job.onet_id)
        else:
            saved_jobs.append(job)
            
    transaction.commit()
    return saved_jobs

def download_feed_file(buid):
    '''
    Downloads the job feed data for a particular job source id.

    '''
    full_file_path = os.path.join(DATA_DIR, FEED_FILE_PREFIX + str(buid) +
                                  '.xml')
    # Download new feed file for today
    logging.info("Downloading new file for BUID %s..." % buid)
    urllib.urlretrieve(generate_feed_url(buid), full_file_path)
    logging.info("Download complete for BUID %s" % buid)
    return full_file_path

def _has_errors(doc):
    has_errors = False
    errors = etree.iterparse(doc, tag='error')
    # we have at least one error, lets deal with it
    for event, error in errors:
        has_errors = True
    return has_errors, errors

def _update_business_unit_modified_dates(buid, crawled_date, updated=True):
    business_unit = BusinessUnit.objects.get(id=buid)
    business_unit.date_crawled = crawled_date
    if updated:
        business_unit.date_updated = datetime.datetime.utcnow()
    business_unit.save()
    
def schedule_jobs(buid):
    parser = etree.XMLParser(no_network=False)
    result = etree.parse(generate_feed_url(buid, 'schedule'), parser)
    try: 
        result.find('confirmation').text
        result = {'success':True}
        logging.info("XML Job Feed - Scheduled for Buid: %s" % buid)
    except AttributeError:
        result = {'success':False, 
                  'error': 'Error: %s' % (result.find('/error/description')\
                                                .text)}
        logging.error(
            "XML Job Feed - Schedule Error", exc_info=sys.exc_info(),
            extra = {"data": {"buid": buid,
                              "error": result}})
    return result
    
def unschedule_jobs(buid):
    parser = etree.XMLParser(no_network=False)
    result = etree.parse(generate_feed_url(buid, 'unschedule'), parser)
    try: 
        result.find('confirmation').text
        result = {'success':True}
        logging.info("XML Job Feed - Unscheduled for Buid: %s" % buid)
    except AttributeError:
        result = {'success':False, 
                  'error': 'Error: %s' % (result.find('/error/description')\
                                                .text)}
        logging.error(
            "XML Job Feed - Unschedule Error", exc_info=sys.exc_info(),
            extra = {"data": {"buid": buid,
                              "error": result}})
    return result

def force_create_jobs(buid):
    parser = etree.XMLParser(no_network=False)
    result = etree.parse(generate_feed_url(buid, 'create'), parser)
    try: 
        result.find('confirmation').text
        result = ("Business unit was created/recreated" 
                  "and will automatically update shortly.")
        logging.info("XML Job Feed - Force create for Buid: %s" % buid)
    except AttributeError:
        result = "Error: %s" % (result.find('/error/description').text)
        logging.error(
            "XML Job Feed - Force Create Error", 
            exc_info=sys.exc_info(),
            extra = {"data": {"buid": buid,
                              "error": result}})
    return result
    
def clear_jobs(buid):
    """
    Delete all jobs for the given BusinessUnit ID.

    Input:
    :buid: Integer. A BusinessUnit ID.

    Returns:
    String reporting status of command.
    
    """
    j = jobListing.objects.filter(buid=buid).delete()
    bu = BusinessUnit.objects.get(id=buid)
    bu.save()

    logging.info("XML Job Feed - Jobs cleared for Buid: %s" % buid)
    return "All jobs for buid %s cleared from system" % (str(buid))

def generate_feed_url(buid, task=None):
    key = settings.FEED_API_KEY
    the_url = ('http://seoxml.directemployers.com/v2/?key=%s&buid=%s' %
               (key, str(buid)))
    if task in ['create', 'schedule', 'unschedule']:
        the_url += '&task=%s' % task
    return the_url

def _build_solr_delete_query(old_jobs):
    if old_jobs:
        delete_query = ("uid:(%s)" % " OR "\
                            .join([str(x) for x in old_jobs]))
    else:
        delete_query = None

    return delete_query

