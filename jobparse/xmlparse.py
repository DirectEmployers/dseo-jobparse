"""
Converts XML feed file into jobListing list for parse_feed_file
in import_jobs.py and into list of dictionaries for Solr

"""

import datetime
import random
import time
from collections import namedtuple
from HTMLParser import HTMLParser
from lxml import etree
from moc_coding import models as moc_models
from slugify import slugify
from templated_emails import utils

from django.dispatch import Signal

from jobparse.models import jobListing


def send_error_notice(sender, **kwargs):
    """
    A receiver to handle ``directseo.xmlparse.feed_error`` signal. When
    a signal is received, it fires off an email to the specified email
    addresses containing what business unit's feed file did not pass
    validation, and the reason why.
    
    """
    return

feed_error = Signal(providing_args=['buid', 'exception', 'line'])
feed_error.connect(send_error_notice)


class JobFeed(object):
    """
    A skeleton for building new translators for job feeds. This class
    should not be invoked directly, only used as a subclass for other
    classes.

    args:
    business_unit -- Either an int/numeric string (e.g. "13"), or a
    BusinessUnit instance. In the first case, the arg willl be used to
    query the database for a BusinessUnit instance.
    filepath -- A string describing the path to the feedfile to be parsed.
    This must be the feed file for the Business Unit referred to by the
    `business_unit` arg.
    co_field -- String. The name of the XML tag containing the name of
    the company the jobs belong to.
    crawl_field -- String. The name of the XML tag containing the datetime
    the feed was crawled.
    node_tag -- String. The name of the XML tag that marks the beginning
    of an XML node that contains all the data for an individual job.
    datetime_pattern -- A string specifying the format of the datetime
    data in the feed. Should conform to the specification outlined here:
    http://docs.python.org/library/time.html#time.strftime
    
    """
    def __init__(self, filepath, co_field=None, crawl_field=None, node_tag=None,
                 datetime_pattern=None):
        if None in (co_field, crawl_field, datetime_pattern):
            raise AttributeError("You must specify valid values for co_field, "
                                 "datetime_pattern and crawl_field.")

        self.filepath = filepath
        self.doc = etree.parse(self.filepath)
        self.datetime_pattern = datetime_pattern
        self.node_tag = node_tag
        self.company = self.parse_doc(co_field)
        self.crawled_date = get_strptime(self.parse_doc(crawl_field),
                                         self.datetime_pattern)
        
    def jobparse(self):
        """
        This method must return a dictionary, where the keys are fields
        on the jobListing model, including foreign key fields. The only
        exceptions to this are any calculated fields. The only such
        fields right now are the 'location' field and any slugfields.

        """
        raise NotImplementedError
        
    def joblist(self):
        return [jobListing(**i) for i in self.jobparse()]

    def solr_job_dict(self, job_node):
        """
        This method must return a dictionary consisting of a mapping
        between fields in the Solr schema (defined in seo.search_indexes)
        and a single job.

        """
        raise NotImplementedError

    def solr_jobs(self):
        """
        This method must return a list of dictionaries from solr_job_dict.

        """
        return [self.solr_job_dict(node) for node in self.jobparse()]

    def job_mocs(self, job):
        """
        Return a list of MOCs and MOC slabs for a given job.
        
        """
        MocData = namedtuple("MocData", "codes slabs ids")
        
        if job['onet_id']:
            mocs = moc_models.Moc.objects.filter(onets=job['onet_id'])
            moc_set = [moc.code for moc in mocs]
            moc_slab = ["%s/%s/%s/vet-jobs::%s - %s" %
                        (slugify(moc.title), moc.code, moc.branch, moc.code,
                         moc.title)
                        for moc in mocs]
            moc_ids = [moc.id for moc in mocs]
            return MocData(moc_set, moc_slab, moc_ids)
        else:
            return MocData(None, None, None)

    def clean_onet(self, onet):
        if onet is None:
            return ""
        return onet.replace("-", "").replace(".", "")

    def parse_doc(self, field, wrapper=None):
        """Use for retrieving document-level (as opposed to job-level) tags."""
        for event, element in etree.iterwalk(self.doc):
            if element.tag == field:
                if wrapper:
                    return wrapper(element.text)
                else:
                    return element.text
        
    def unescape(self, val):
        h = HTMLParser()

        if val:
            return h.unescape(val.strip())

    def full_loc(self, obj):
        fields = ['city', 'state', 'location', 'country']
        strings = ['%s::%s' % (f, obj[f]) for f in fields]
        
        return '@@'.join(strings)
        
    def country_slab(self, obj):
        return "%s/jobs::%s" % (obj['country_short'].lower(), obj['country'])

    def state_slab(self, obj):
        if slugify(obj['state']):
            url = "%s/%s/jobs" % (slugify(obj['state']),
                                  obj['country_short'].lower())
            
            return "%s::%s" % (url, obj['state'])

    def city_slab(self, obj):
        url = "%s/%s/%s/jobs" % (slugify(obj['city']), slugify(obj['state']), 
                                 obj['country_short'].lower())
        return "%s::%s" % (url, obj['location'])

    def title_slab(self, obj):
        if slugify(obj['title']) and slugify(obj['title']) != "none":
            return "%s/jobs-in::%s" % (slugify(obj['title']).strip('-'),
                                       obj['title'])

    def co_slab(self):
        return  u"{cs}/careers::{cn}".format(cs=slugify(self.company),
                                             cn=self.company)


class DEJobFeed(JobFeed):
    def __init__(self, *args, **kwargs):
        kwargs.update({
            'crawl_field': 'date_modified',
            'node_tag': 'jobs',
            'datetime_pattern': '%m/%d/%Y %I:%M:%S %p'
        })
        super(DEJobFeed, self).__init__(*args, **kwargs)

    def date_salt(self, date):
        """
        Generate a new datetime value salted with a random value, so that
        jobs will not be clumped together by job_source_id on the job list
        pages. This time is constrained to between `date` and the
        previous midnight so that jobs that are new on a given day don't
        wind up showing up on the totally wrong day inadvertently.

        Input:
        :date: A `datetime.datetime` object. Represents the date a job
        was posted.

        Returns:
        A datetime object representing a random time between `date` and
        the previous midnight.
        
        """
        oneday = datetime.timedelta(hours=23, minutes=59, seconds=59)
        # midnight last night
        lastnight = datetime.datetime(date.year, date.month, date.day)
        # midnight tonight
        tonight = lastnight + oneday
        # seconds since midnight last night
        start = (date - lastnight).seconds
        # seconds until midnight tonight
        end = (tonight - date).seconds
        # Number of seconds between 'date' and the previous midnight.
        salt = random.randrange(-start, end)
        # seconds elapsed from epoch to 'date'
        seconds = time.mktime(date.timetuple())
        # Convert milliseconds -> time tuple
        salted_time = time.localtime(seconds + salt)
        # `salted_time` at this point is a time tuple, which has the same API
        # as a normal tuple. We destructure it and pass only the first six
        # elements (year,month,day,hour,min,sec).
        return datetime.datetime(*salted_time[0:6])
        
    def solr_job_dict(self, job_node):
        job_dict = {}
    
        if job_node['city'] and job_node['state_short']:
            job_node['location'] = job_node['city'] + ', ' + job_node['state_short']
        elif job_node['city'] and job_node['country_short']:
            job_node['location'] = job_node['city'] + ', ' + job_node['country_short']
        elif job_node['state'] and job_node['country_short']:
            job_node['location'] = job_node['state'] + ', ' + job_node['country_short']
        elif job_node['country']:
            job_node['location'] = 'Virtual, ' + job_node['country_short']
        else:
            job_node['location'] = 'Global'

        country_slab = self.country_slab(job_node)
        company_slab = self.co_slab()
        city_slab = self.city_slab(job_node)
        state_slab = self.state_slab(job_node)
        title_slab = self.title_slab(job_node)
        mocdata = self.job_mocs(job_node)
        
        job_dict['buid'] = job_node['buid_id']
        job_dict['city'] = job_node['city']
        job_dict['city_ac'] = job_node['city']
        job_dict['city_exact'] = job_node['city']
        job_dict['city_slab'] = city_slab
        job_dict['city_slab_exact'] = city_slab
        job_dict['city_slug'] = slugify(job_node['city'])
        job_dict['company'] = self.company
        job_dict['company_ac'] = self.company
        job_dict['company_exact'] = self.company
        job_dict['company_slab'] = company_slab
        job_dict['company_slab_exact'] = company_slab
        job_dict['country'] = job_node['country']
        job_dict['country_ac'] = job_node['country']
        job_dict['country_exact'] = job_node['country']
        job_dict['country_short'] = job_node['country_short']
        job_dict['country_slab'] = country_slab
        job_dict['country_slab_exact'] = country_slab
        job_dict['country_slug'] = slugify(job_node['country'])
        job_dict['date_new'] = job_node['date_new']
        job_dict['date_new_exact'] = job_node['date_new']
        job_dict['date_updated'] = job_node['date_updated']
        job_dict['date_updated_exact'] = job_node['date_updated']
        job_dict['description'] = job_node['description']
        job_dict['full_loc'] = self.full_loc(job_node)
        job_dict['full_loc_exact'] = self.full_loc(job_node)
        job_dict['location'] = job_node['location']
        job_dict['location_exact'] = job_node.get('location')
        job_dict['moc'], job_dict['moc_exact'] = mocdata.codes, mocdata.codes
        job_dict['moc_slab'], job_dict['moc_slab_exact'] = (mocdata.slabs,
                                                            mocdata.slabs)
        job_dict['mocid'] = mocdata.ids
        job_dict['onet'] = self.clean_onet(job_node['onet_id'])
        job_dict['onet_exact'] = self.clean_onet(job_node['onet_id'])
        job_dict['reqid'] = job_node['reqid']
        job_dict['salted_date'] = self.date_salt(job_node['date_updated'])
        job_dict['state'] = job_node['state']
        job_dict['state_ac'] = job_node['state']
        job_dict['state_exact'] = job_node['state']
        job_dict['state_short'] = job_node['state_short']
        job_dict['state_slab'] = state_slab
        job_dict['state_slab_exact'] = state_slab
        job_dict['state_slug'] = slugify(job_node['state'])
        job_dict['title'] = job_node['title']
        job_dict['title_ac'] = job_node['title']
        job_dict['title_exact'] = job_node['title']
        job_dict['title_slab'] = title_slab
        job_dict['title_slab_exact'] = title_slab
        job_dict['title_slug'] = slugify(job_node['title'])
        job_dict['uid'] = job_node['uid']
        job_dict['zipcode'] = job_node['zipcode']

        # Custom fields defined originally as part of Haystack and incorporated
        # into our application. Except 'id', which is the uniqueKey for our
        # index (think primary key for a database).
        job_dict['id'] = 'seo.joblisting.' + job_dict['uid']
        job_dict['django_id'] = 0
        job_dict['django_ct'] = 'seo.joblisting'
        job_dict['text'] = " ".join([(job_dict.get(k) or "None") for k
                                     in ['description', 'title', 'country',
                                         'country_short', 'state', 'state_short',
                                         'city']])
        return job_dict


class DEv1JobFeed(DEJobFeed):
    """
    Transform an XML feed file from DirectEmployers Foundation into database-
    and Solr-ready data structures.

    """
    def __init__(self, *args, **kwargs):
        kwargs.update({'co_field': 'business_unit_name'})
        super(DEv1JobFeed, self).__init__(*args, **kwargs)
    
    def jobparse(self):
        jobs = self.doc.find("jobs").iterchildren()
        joblist=[]
        for job in jobs:
            jobdict = {}
            
            for attribute in job:
                if attribute.tag == 'u_id':
                    jobdict['uid'] = attribute.text
                elif attribute.tag == 'onets':
                    onet = attribute.find('onet')

                    if onet is not None:
                        jobdict['onet_id'] = onet.findtext('onet_code')
                    else:
                        jobdict['onet_id'] = None
                        
                elif attribute.tag == 'buid':
                    jobdict['buid_id'] = attribute.text
                elif attribute.tag == 'location':
                    jobdict['country_short'] = attribute.findtext('country_short') or None
                    jobdict['country'] = attribute.findtext('country') or None
                    jobdict['state_short'] = attribute.findtext('state_short') or None
                    jobdict['state'] = attribute.findtext('state') or None
                    jobdict['city'] = attribute.findtext('city') or None
                elif attribute.tag in ('description', 'city', 'state', 'title',
                                       'country'):
                    jobdict[attribute.tag] = self.unescape(attribute.text)
                elif attribute.tag.startswith("date_"):
                    jobdict[attribute.tag] = get_strptime(attribute.text,
                                                          self.datetime_pattern)
                else:
                    jobdict[attribute.tag] = attribute.text
                    
            joblist.append(jobdict)

        return joblist


class DEv2JobFeed(DEJobFeed):
    """
    Transform an XML feed file from DirectEmployers Foundation into database-
    and Solr-ready data structures.

    """
    def __init__(self, *args, **kwargs):
        kwargs.update({'co_field': 'job_source_name'})
        super(DEv2JobFeed, self).__init__(*args, **kwargs)
        jsid = self.parse_doc("job_source_id")

        if jsid:
            self.jsid = int(jsid)
        else:
            self.jsid = 0

        self.errors = False
        self.error_messages = None
        self.schema = etree.XMLSchema(etree.parse("feed_schema.xsd"))

        if not self.schema.validate(self.doc):
            exc = self.schema.error_log.last_error
            self.error_messages = {'exception': exc.message, 'line': exc.line,
                                   'buid': self.jsid}
            feed_error.send(sender=self, **self.error_messages)
            self.errors = True

    def jobparse(self):
        fieldmap = {}
        joblist=[]
        # Collection of all jobListing attribute names that are the same as in
        # the feed.
        fields = ('city', 'country', 'country_short', 'state', 'state_short',
                  'title', 'uid', 'reqid', 'link', 'description', 'hitkey',
                  'zipcode')

        for field in fields:
            fieldmap[field] = field

        fieldmap['onet_id'] = 'onet_code'
        fieldmap['buid_id'] = self.jsid
        fieldmap['date_new'] = 'date_created'
        fieldmap['date_updated'] = 'date_modified'
        
        jobs = self.doc.find(self.node_tag).iterchildren()

        for job in jobs:
            jobdict = {}
            for key, value in fieldmap.items():

                # Since buid_id is a static value we get from the top leve of
                # the XML document, we just want to set the value directly,
                # then ``continue`` through the fieldmap for-loop.
                if key == 'buid_id':
                    jobdict[key] = value
                    continue
                elif key == 'zipcode':
                    attr = job.find('zip')
                else:
                    attr = job.find(value)

                if key in ('date_new', 'date_updated'):
                    jobdict[key] = get_strptime(attr.text, self.datetime_pattern)
                elif key == "onet_id":
                    jobdict[key] = self.clean_onet(attr.text)
                else:
                    jobdict[key] = attr.text
            
            joblist.append(jobdict)
        return joblist

    
def get_strptime(ts, pattern):
    """Convert a datetime string to a datetime object."""
    if not ts:
        return None
    else:
        return datetime.datetime.fromtimestamp(time.mktime(
            time.strptime(ts, pattern)))

