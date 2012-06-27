# -*- coding: utf-8 -*-
import os

from django.conf import settings
from django.test import TestCase

from jobparse import import_jobs
from ..models import BusinessUnit
from .factories import BusinessUnitFactory


class ImportJobsTestCase(TestCase):
    def setUp(self):
        super(ImportJobsTestCase, self).setUp()
        self.businessunit = BusinessUnitFactory.build()
        self.businessunit.save()
        self.buid_id = self.businessunit.id        
        self.filepath = os.path.join(settings.DATA_DIR,
                                     'dseo_feed_%s.xml' % self.buid_id)

    def test_solr_rm_feedfile(self):
        """
        Test that at the end of Solr parsing, the feed file is deleted.
        
        """
        import_jobs.update_solr(self.buid_id)
        self.assertFalse(os.access(self.filepath, os.F_OK))

    def test_db_rm_feedfile(self):
        """
        Test that at the end of parsing a feed file for the database, the
        feed file is deleted.

        """
        import_jobs.download_feed_file(self.buid_id)
        import_jobs.parse_feed_file(self.filepath, self.buid_id)
        self.assertFalse(os.access(self.filepath, os.F_OK))
        import_jobs.download_feed_file(self.buid_id)

    def test_set_bu_title(self):
        """
        Ensure that if a feedfile for a BusinessUnit comes through, and
        the `title` attribute for that BusinessUnit is not set, that
        `helpers.update_solr` sets the `title` attribute properly.

        """
        bu = BusinessUnit.objects.get(id=self.buid_id)
        bu.title = None
        bu.save()
        # Since the BusinessUnit title is None, the intent is that update_solr
        # will set its title to match the company name found in the feed file.
        results = import_jobs.update_solr(self.buid_id)
        # We have to get the updated state of the BusinessUnit instance, since
        # changes to the database won't be reflected by our in-memory version of
        # the data.
        bu = BusinessUnit.objects.get(id=self.buid_id)
        # The title attribute should now equal the initial value established in
        # the setUp method.
        self.assertEquals(self.businessunit.title, bu.title)

        

