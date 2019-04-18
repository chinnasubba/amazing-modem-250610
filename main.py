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
    
    # decode event paylod
    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    pubsub_message = json.loads(pubsub_message)
    
    # set up alert message format; this is where you would like to customize your alert message:
    alert_dict = dict()
    alert_dict['log_id'] = pubsub_message['insertId']
    alert_dict['jsonPayload'] = pubsub_message['jsonPayload']
    alert_dict['severity'] = pubsub_message['severity']
    alert_dict['logName'] = pubsub_message['logName']
    
    # convert timezone to PST
    ts = datetime.strptime(pubsub_message['receiveTimestamp'][:-4], "%Y-%m-%dT%H:%M:%S.%f")
    new_ts = datetime.strftime(ts + timedelta(hours=-7), "%Y-%m-%dT%H:%M:%S.%f")
    alert_dict['receiveTimestamp'] = new_ts

    # only push ERROR or WARNING message to Slack
    if alert_dict['severity'] == "ERROR" or alert_dict['severity'] == "WARNING":
        new_msg_dict = {"text": f"{alert_dict}"}
        new_msg = json.dumps(new_msg_dict)

        # set up jinja template
        t = Template('{"attachments":[{"title":"StackDriver Alerts on fivetran Projects", "mrkdwn_in": ["text","fields"], "text": {{str_var}}}]}')

        payload = t.render(str_var=new_msg)
        response = requests.post('https://hooks.slack.com/services/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', headers=headers, data=payload)
        
    else:
        return None