import base64
import requests
import json
from datetime import timedelta
from datetime import datetime
from jinja2 import Template

def inject_to_slack(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # define header for slack incoming webhook
    headers = {
        'Content-type': 'application/json',
    }  
    slack_url = 'https://hooks.slack.com/services/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'
    
    # decode event paylod
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message = json.loads(pubsub_message)
    
    # set up alert message format; this is where you would like to customize your alert message:
    alert_dict = dict()
    alert_dict['log_id'] = pubsub_message['insertId']
    alert_dict['connector_id'] = pubsub_message['jsonPayload']['connector_id']
    alert_dict['connector_type'] = pubsub_message['jsonPayload']['connector_type']
    if 'data' in pubsub_message['jsonPayload'].keys():
        alert_dict['jsonPayload_data'] = pubsub_message['jsonPayload']['data']

    alert_dict['severity'] = pubsub_message['severity']
    alert_dict['logName'] = pubsub_message['logName']
    
    # convert timezone to PST
    ts = datetime.strptime(pubsub_message['receiveTimestamp'][:-4], "%Y-%m-%dT%H:%M:%S.%f")
    new_ts = datetime.strftime(ts + timedelta(hours=-7), "%Y-%m-%dT%H:%M:%S.%f")
    alert_dict['receiveTimestamp'] = new_ts

    # only push ERROR or WARNING message to Slack
    if alert_dict['severity'] == "ERROR": # or alert_dict['severity'] == "WARNING"

        # set up jinja template
        t = Template('{"attachments":[{"title":"StackDriver Alerts from fivetran connector: {{source}}", "mrkdwn_in": ["text","fields"], "text": "{{str_var}}"}]}')
        payload = t.render(source=alert_dict['connector_id'], str_var=alert_dict)
        
        response = requests.post(slack_url, headers=headers, data=payload)
        
    else:
        return None