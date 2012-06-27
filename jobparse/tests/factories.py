import factory
from slugify import slugify
from ..models import *


class jobListingFactory(factory.Factory):
    FACTORY_FOR = jobListing

    buid_id = 1
    city = "Whitehouse Station"
    citySlug = factory.LazyAttribute(lambda x: slugify(x.city))
    country = "United States"
    countrySlug = factory.LazyAttribute(lambda x: slugify(x.country))
    country_short = "USA"
    date_new = "2010-09-02 00:24:31"
    date_updated = "2012-02-27 08:43:39"
    description = "This is description #1"
    hitkey = "GEN000110"
    id = 39557
    link = "http://jcnlx.com/CE87A7DC932C49ED916171F5806C37C510"
    onet_id = None
    reqid = "GEN000110"
    state = "New Jersey"
    stateSlug = factory.LazyAttribute(lambda x: slugify(x.state))
    state_short = "NJ"
    title = "Senior Internal Auditor"
    titleSlug = factory.LazyAttribute(lambda x: slugify(x.title))
    uid = 17059006

    def __init__(self, *args, **kwargs):
        super(jobListingFactory, self).__init__(*args, **kwargs)
        self.location()

    def location(self):
        if self.city and self.state_short:
            self.location = ', '.join(self.city, self.state_short)
        elif self.city and self.country_short:
            self.location = ', '.join(self.city, self.country_short)
        elif self.state and self.country_short:
            self.location = ', '.join(self.state, self.country_short)
        elif self.country:
            self.location = ', '.join('Virtual', self.country_short)
        else:
            self.location = 'Global'


class BusinessUnitFactory(factory.Factory):
    FACTORY_FOR = BusinessUnit

    id = 0
    title = "HSBC"
    title_slug = factory.LazyAttribute(lambda x: slugify(x.title))
    date_updated = "2010-10-18 10:59:24"
    associated_jobs = 4
    date_crawled = "2010-10-18 07:00:02"
    veteran_commit = True

