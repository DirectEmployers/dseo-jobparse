from django.contrib.contenttypes import generic
from django.db import models
from slugify import slugify

from moc_coding import models as moc_models

class jobListing(models.Model):
    def __unicode__(self):
        return self.title
        
    class Meta:
        verbose_name = 'Job Detail'
        verbose_name_plural = 'Job Details'

    buid = models.ForeignKey('BusinessUnit')
    city = models.CharField(max_length=200, blank=True, null=True)
    citySlug = models.SlugField(blank=True, null=True)
    country = models.CharField(max_length=200, blank=True, null=True)
    countrySlug = models.SlugField(blank=True, null=True)
    country_short = models.CharField(max_length=3, blank=True, null=True,
                                     db_index=True)
    date_new = models.DateTimeField('date new')
    date_updated = models.DateTimeField('date updated')
    description = models.TextField()
    hitkey = models.CharField(max_length=50)
    link = models.URLField(max_length=200)
    location = models.CharField(max_length=200, blank=True, null=True)
    onet = models.ForeignKey(moc_models.Onet, blank=True, null=True)
    reqid = models.CharField(max_length=50, blank=True, null=True)
    state = models.CharField(max_length=200, blank=True, null=True)
    stateSlug = models.SlugField(blank=True, null=True)
    state_short = models.CharField(max_length=3, blank=True, null=True)
    title = models.CharField(max_length=200)
    titleSlug = models.SlugField(max_length=200, blank=True, null=True,
                                 db_index=True)
    uid = models.IntegerField(db_index=True, unique=True)
    zipcode = models.CharField(max_length=15, null=True, blank=True)

    def return_id(self):
        return self.id

    def save(self):
        self.titleSlug = slugify(self.title)
        self.countrySlug = slugify(self.country)
        self.stateSlug = slugify(self.state)
        self.citySlug = slugify(self.city)
        
        if self.city and self.state_short:
            self.location = self.city + ', ' + self.state_short
        elif self.city and self.country_short:
            self.location = self.city + ', ' + self.country_short
        elif self.state and self.country_short:
            self.location = self.state + ', ' + self.country_short
        elif self.country:
            self.location = 'Virtual, ' + self.country_short
        else:
            self.location = 'Global'
            
        super(jobListing, self).save()


class BusinessUnit(models.Model):
    """
    A BusinessUnit is a source of job posting data. It may be an
    Applicant Tracking System (ATS) like Taleo or JobFox, it may be a
    member company like IBM or Lockheed Martin, a state job bank, or any
    number of other possibilities. In other words, it is a model we use
    to uniquely identify a job feed, and to store metadata about that
    feed (e.g. date last crawled, etc.). 
    
    """
    def __unicode__(self):
        return "%s: %s" % (self.title, str(self.id))
        
    class Meta:
        verbose_name = 'Business Unit'
        verbose_name_plural = 'Business Units'
        
    def save(self, *args, **kwargs):
        self.associated_jobs = jobListing.objects.filter(buid=self.id).count()
        self.title_slug = slugify(self.title)
        super(BusinessUnit, self).save(*args, **kwargs)

    def show_sites(self):
        sites_list = ""
        for site in self.seosite_set.all():
            sites_list += "%s, " % site.domain
        return sites_list[0:len(sites_list)-2]        
        
    id = models.IntegerField('Business Unit ID', max_length=10,
                             primary_key=True)
    title = models.CharField(max_length=50, null=True, blank=True)
    title_slug = models.SlugField(null=True, blank=True)    
    date_crawled = models.DateTimeField('Date Crawled')
    date_updated = models.DateTimeField('Date Updated')
    associated_jobs = models.IntegerField('Associated Jobs', default=0)
    veteran_commit = models.BooleanField('Veteran Commit', default=True)
    customcareers = generic.GenericRelation(moc_models.CustomCareer)

