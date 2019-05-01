import base64
import requests
import json
from datetime import timedelta
from datetime import datetime
from jinja2 import Template
from google.cloud import storage


# this CF takes fivetran logs pub/sub and transform to messages for Slack webhook ingestion
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


# this CF takes airflow logs that are being stored in Cloud Storage and send to StackDriver only the traceback errors 
def airflow_handler(data, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    # define header for slack incoming webhook
    headers = {
        'Content-type': 'application/json',
    }  
    slack_url = 'https://hooks.slack.com/services/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'

    client = storage.Client()
    bucket = client.get_bucket(format(data['bucket']))
    blob = bucket.get_blob(format(data['name']))

    blob_str = blob.download_as_string()
    new_blob_str = blob_str.decode('utf-8')
    new_blob_list = new_blob_str.split('\n')

    # extracting trace errors
    for count, line in enumerate(new_blob_list):
        counter = count
        traceback = []

        if 'ERROR' in line:
            for sub_count, sub_line in enumerate(new_blob_list, counter):
                
                if ('INFO' not in sub_line) and (' /tmp' not in sub_line) and (sub_line[0:3] != '---') and (sub_line[-4:-1] != '---') \
                and ('AIRFLOW_' not in sub_line):
                    traceback.append(sub_line)
            break
    
    blob_name = blob.name
    log_array = blob_name.split('/')

    # checking the logging_mixin output (last line of log)
    if 'Task exited with return code 1' in new_blob_list[-2]:
        # set up alert message format; this is where you would like to customize your alert message:
        alert_dict = dict()
        alert_dict['dag_id'] = log_array[0]
        alert_dict['task_id'] = log_array[1]
        alert_dict['attempts'] = log_array[3][0]
        alert_dict['log_details'] = traceback
        ts = datetime.strptime(log_array[2][:-6], "%Y-%m-%dT%H:%M:%S")
        new_ts = datetime.strftime(ts + timedelta(hours=-6), "%Y-%m-%dT%H:%M:%S")
        alert_dict['exec_ts'] = new_ts
        new_alert_dict = '\n '.join(alert_dict['log_details'])
        newer_alert_dict = new_alert_dict.replace("\"", "'")
        conv_alert_dict = newer_alert_dict.replace('{}', '{{}}')

        pretty_msg = """
            :red_circle: Airflow DAG Failed.  
            *DAG ID*: {dagId} 
            *Task ID*: {taskId}
            *Attempts of Retries*: {attempts}
            *Execution Timestamp*: {execTimestamp}
            *Log Details*: 
            {logDetails}
            *Severity*: {severity}
            *Airflow DAG Status URL*: {url}
            """.format(
            dagId=alert_dict['dag_id'],
            taskId=alert_dict['task_id'],
            attempts=alert_dict['attempts'],
            execTimestamp=alert_dict['exec_ts'],
            logDetails=conv_alert_dict,
            severity='ERROR',
            url=f"http://34.83.69.168:8080/admin/airflow/tree?dag_id={alert_dict['dag_id']}")

        t = Template('{"text": "{{pretty_msg}}"}')
        pretty_payload = t.render(pretty_msg=pretty_msg)
        response = requests.post(slack_url, headers=headers, data=pretty_payload)
        print(response.text)
