from django.contrib import admin

from jobparse.forms import BusinessUnitForm
from jobparse.models import BusinessUnit


class BusinessUnitAdmin(admin.ModelAdmin):
    list_display = ('__unicode__', 'show_sites', 'associated_jobs',
                    'date_crawled', 'date_updated')
    actions = ['reset_jobs', 'force_create', 'clear']
    readonly_fields = ('associated_jobs',)
    form = BusinessUnitForm
    search_fields = ['seosite__domain', 'seosite__name', 'title']
    prepopulated_fields = {'title_slug': ('title',)}
    fieldsets = [
        ('Basics', {'fields': [('id', 'title', 'title_slug',
                                'associated_jobs'),
                               ('date_crawled', 'date_updated'),
                               ('veteran_commit')]}),
        ('Sites', {'fields': ['sites']})
    ]
        
    def reset_jobs(self, request, queryset):
        """
        Sends a message via Celery to download & parse the feedfile for a
        given Business Unit, then write the results to the Solr index and
        the RDBMS.

        """
        for business_unit in queryset:
            tasks.task_refresh_bunit_jobs.delay(business_unit.id,
                                                update_all=True)
            tasks.task_update_solr.delay(business_unit.id, force=True)
            
        messages.info(request, "All jobs for Business Unit %s will be "
                      "re-processed shortly." % business_unit.id)

    reset_jobs.short_description = "Refresh all jobs in business unit"
    
    def clear(self, request, queryset):
        for jsid in queryset:
            tasks.task_clear.delay(jsid)
            tasks.task_clear_solr.delay(jsid.id)
            
        messages.info(request, "All jobs for Business Unit:%s will be "
                      "removed shortly." % jsid.id)
    clear.short_description = "Clear jobs from business unit"

    def save_model(self, request, obj, form, change):
        obj.save()
        # Grab the scheduled message appended on to cleaned_data
        # to pass to message_user() in order to report on the status
        # of the call to import_jobs.(un)schedule_jobs()
        scheduled_message = form.cleaned_data['scheduled_message']
        if scheduled_message:
            messages.info(request, "%s: %s" % (obj, scheduled_message))


admin.site.register(BusinessUnit, BusinessUnitAdmin)

