# -*- coding: utf-8 -*-
import os.path
import shutil
import datetime

from django.conf import settings
from django.test import TestCase

from pysolr import Solr

from jobparse import import_jobs, xmlparse
from ..models import BusinessUnit, jobListing
from .factories import BusinessUnitFactory


class JobFeedTestCase(TestCase):

    def setUp(self):
        super(JobFeedTestCase, self).setUp()
        self.businessunit = BusinessUnitFactory.build()
        self.businessunit.save()
        self.buid_id = self.businessunit.id
        self.numjobs = 4
        self.testdir = os.path.abspath(os.path.dirname(__file__))
        self.conn = Solr("http://127.0.0.1:8983/solr/")
        self.emptyfeed = os.path.join(self.testdir, "dseo_feed_0.no_jobs.xml")

        #Ensures DATA_DIR used by import_jobs.download_feed_file exists
        data_path = settings.DATA_DIR
        if not os.path.exists(data_path):
            os.mkdir(data_path)
        
    def test_dev2_feed(self):
        filepath = import_jobs.download_feed_file(self.buid_id)
        results = xmlparse.DEv2JobFeed(filepath)
        jobs = results.jobparse()
        self.assertEqual(results.jsid, self.buid_id)
        self.assertEqual(results.company, self.businessunit.title)
        self.assertEqual(len(jobs), self.numjobs)
        # Test for the presence of every non-calculated field on the jobListing
        # model. (That is, all slugfields and 'location' are left out.)
        self.assertEqual(set(jobs[0].keys()), set(['buid_id', 'city', 'country',
                                                   'country_short', 'date_new',
                                                   'date_updated', 'description',
                                                   'hitkey', 'link', 'onet_id',
                                                   'reqid', 'state',
                                                   'state_short', 'title',
                                                   'uid', 'zipcode']))

    def test_mocids(self):
        """
        Tests that mocid fields exist when jobs are imported from a feed and
        added to a solr connnection
        
        """
        filepath = import_jobs.download_feed_file(self.buid_id)
        results = xmlparse.DEv2JobFeed(filepath)
        jobs = results.solr_jobs()
        # Since we're going to be adding/updating data in the Solr index, we're
        # hardcoding in the local Solr instance so that we don't accidentally
        # alter production data.
        self.conn.add(jobs)
        num_hits = self.conn.search(q="*:*",
                                    fq="buid:%s -mocid:[* TO *]" % self.buid_id)
        self.assertEqual(num_hits.hits, self.numjobs)
        for job in jobs:
            self.assertTrue('mocid' in job)

    def test_empty_feed(self):
        """
        Test that the schema for the v2 DirectEmployers feed file schema
        allows for empty feed files.
        
        """
        results = xmlparse.DEv2JobFeed(self.emptyfeed)
        # If the schema is such that empty feed files are considered invalid,
        # trying to run jobparse() will throw an exception.
        self.assertEqual(len(results.jobparse()), 0)

    def test_empty_solr(self):
        """
        Tests for the proper behavior when encountering a job-less, but
        otherwise valid, feed file. The proper behavior is to delete any
        jobs associated with that BusinessUnit from the Solr index.

        """
        # Normal download-and-parse operation on a feed file with jobs.
        import_jobs.update_solr(self.buid_id)
        results = self.conn.search(q="*:*", fq="buid:%s" % self.buid_id)
        self.assertEqual(results.hits, self.numjobs)

        # Download-and-parse operation on a feed file with no jobs. Expected
        # behavior is to delete all jobs.
        self._get_feedfile()
        import_jobs.update_solr(self.buid_id, download=False)
        results = self.conn.search(q="*:*", fq="buid:%s" % self.buid_id)
        self.assertEqual(results.hits, 0)

    def test_empty_db(self):
        """
        Tests for the proper behavior when encountering a job-less, but
        otherwise valid, feed file. The proper behavior is to delete any
        jobs associated with that BusinessUnit from the database.

        """
        # Normal download-and-parse operation on a feed file with jobs.
        import_jobs.refresh_bunit_jobs(self.buid_id)
        dbjobs = jobListing.objects.filter(buid=self.buid_id).count()
        self.assertEqual(dbjobs, self.numjobs)
        
        # Download-and-parse operation on a feed file with no jobs. Expected
        # behavior is to delete all jobs.
        self._get_feedfile()
        import_jobs.refresh_bunit_jobs(self.buid_id, download=False)
        dbjobs = jobListing.objects.filter(buid=self.buid_id).count()
        self.assertEqual(dbjobs, 0)

    def test_zipcode(self):
        """
        Tests to ensure proper behavior of zipcode field in being entered both
        in the database and Solr.

        """
        filepath = import_jobs.download_feed_file(self.buid_id)
        dbresults = xmlparse.DEv2JobFeed(filepath)
        solrresults = dbresults.solr_jobs()

        zips_from_feedfile = ["28243", "10095", "90212", "30309"]
        solrzips = [i['zipcode'] for i in solrresults]
        dbzips = [i['zipcode'] for i in dbresults.jobparse()]
        
        for coll in [solrzips, dbzips]:
            self.assertItemsEqual(zips_from_feedfile, coll)

    def test_salt_date(self):
        """
        Test to ensure that job postings show up in a quasi-random
        fashion by sorting by the `salted_date` attribute in the index
        vice strictly by `date_new`.
        
        """
        filepath = import_jobs.download_feed_file(self.buid_id)
        jobs = xmlparse.DEv2JobFeed(filepath)
        solrjobs = jobs.solr_jobs()
        self.conn.add(solrjobs)
        results = self.conn.search(q="*:*", sort="salted_date asc")
        self.assertEqual(self.numjobs, results.hits)
        # We can't really test for inequality between the two result sets,
        # since sometimes results.docs will equal results2.docs.
        results2 = self.conn.search(q="*:*", sort="date_new asc")
        self.assertItemsEqual(results2.docs, results.docs)

    def test_date_updated(self):
        """
        Test to ensure proper behavior of date updated field when added to
        Solr.

        """
        filepath = import_jobs.download_feed_file(self.buid_id)
        jobs = xmlparse.DEv2JobFeed(filepath)
        solrjobs = jobs.solr_jobs()
        self.conn.add(solrjobs)
        date_updated = datetime.datetime.strptime("5/17/2012 12:01:05 PM",
                                                  "%m/%d/%Y %I:%M:%S %p")
        solr_dates = [i['date_updated'] for i in solrjobs]

        for solr_date in solr_dates:
            self.assertEqual(solr_date, date_updated)
        
    def _get_feedfile(self):
        # Download the 'real' feed file then copy the empty feed file in its
        # place.
        realfeed = import_jobs.download_feed_file(self.buid_id)
        shutil.copyfile(realfeed, "%s.bak" % realfeed)
        shutil.copyfile(self.emptyfeed, realfeed)

