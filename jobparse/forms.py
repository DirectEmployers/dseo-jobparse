from django import forms

from directseo.seo import models as seo_models
from jobparse.models import BusinessUnit

class BusinessUnitForm(forms.ModelForm):
    sites = forms.ModelMultipleChoiceField(seo_models.SeoSite.objects.all(),
                                           required=False)
    def __init__(self, *args, **kwargs):
        forms.ModelForm.__init__(self, *args, **kwargs)
        initial = {}
        sites = seo_models.SeoSite.objects.all().order_by('domain')
        dictionary = {'queryset': sites,
                      'widget': admin.widgets.FilteredSelectMultiple('Sites',
                                                                     False),
                      'initial': initial, 
                      'required': False}
        if 'instance' in kwargs:
            for site in sites:
                if site in kwargs['instance'].seosite_set.all():
                    initial[str(site.id)] = 'selected'
            dictionary['initial'] = initial
        self.fields['sites'] = forms.ModelMultipleChoiceField(**dictionary)

    def save(self, commit=True):
        added_sites = set()
        business_unit = forms.ModelForm.save(self, commit)
        for site in self.cleaned_data['sites']:
            added_sites.add(site)
        if business_unit.pk:
            if set(added_sites) != set(business_unit.seosite_set.all()):
                business_unit.seosite_set = added_sites
        else:
            business_unit.save()
            business_unit.seosite_set = added_sites
        return business_unit
        
    class Meta:
        model = BusinessUnit

