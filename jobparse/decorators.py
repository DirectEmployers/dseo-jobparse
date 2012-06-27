import logging
import urllib2

from django.http import HttpResponse
from django.utils import simplejson
from django.views.decorators.csrf import csrf_exempt

@csrf_exempt
def sns_json_message(f): 
    
    def wrap(request, *args, **kwargs):      
        # check the request to see if we need to confirm subscription
        json = None
        res = request.raw_post_data
        try:
            json = simplejson.JSONDecoder().decode(res)
            if json["Type"] == "SubscriptionConfirmation":
                subscribeURL = json["SubscribeURL"]
                res = urllib2.urlopen(subscribeURL)
        except KeyError:
            # We 'pass' here because the Type attribute might not be
            # in the JSON object if we've already subscribed to the 
            # end point.
            pass
        except Exception as e:
            logging.error("%s" % e, 
                          exc_info=sys.exc_info(), 
                          extra={'request': request,
                                 'view': 'sns_json_message decorator'})
            return HttpResponse(status=500)
        finally:
            f(json, *args, **kwargs)
            return HttpResponse(status=200)
    
    return wrap

