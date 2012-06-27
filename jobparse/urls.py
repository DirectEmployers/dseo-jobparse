import logging

import tasks
from jobparse.decorators import sns_json_message

LOG = logging.getLogger('jobparse.views')

@sns_json_message
def send_sns_confirm(response):
    """
    Receive 'ping' from Amazon SNS that an XML feed is ready for parsing,
    then dispatch tasks to parse that file for entry into both Solr and
    the RDBMS.
    
    """
    LOG.info("sns received", extra = {
        'view': 'send_sns_confirm',
        'data': {
            'json message': response
        } 
    })
    if response:
        # 'buid' is an integer representing the ID of the business unit.
        buid = response['Subject']
        tasks.task_refresh_bunit_jobs.delay(buid, update_all=True)
        tasks.task_update_solr.delay(buid, force=True)
