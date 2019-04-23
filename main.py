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
        # WARNING: payload changes for every severity type. Need to change the keys of alert_dict['jsonPayload_data]

        # set up jinja template
        id_array = alert_dict['logName'].split("/")[-1].split("-")

        pretty_msg = """
            :red_circle: Connector Failed.  
            *Project*: {proj} 
            *Connector Type*: {connType}
            *Connector Schema*: {connSchema}
            *Alert Reason*: {reason}
            *Alert Status*: {status}
            *StackDriver Log ID*: {logId}
            *Received Timestamp*: {receivedAtTimestamp}
            *Severity*: {severity}
            *fivetran Dash URL*: {dash_url}
            """.format(
            proj=id_array[1],
            connType=alert_dict['connector_type'],
            connSchema=alert_dict['connector_id'],
            reason=alert_dict['jsonPayload_data']['reason'],
            status=alert_dict['jsonPayload_data']['status'],
            logId=alert_dict['log_id'],
            receivedAtTimestamp=alert_dict['receiveTimestamp'],
            severity=alert_dict['severity'],
            dash_url=f"https://fivetran.com/dashboard/connectors/{id_array[2]}/{alert_dict['connector_id']}"
            )

        t = Template('{"text": "{{pretty_msg}}"}')
        pretty_payload = t.render(pretty_msg=pretty_msg)

        response = requests.post(slack_url, headers=headers, data=pretty_payload)
        
    else:
        return None