# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either expressed or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Retrieves Google Analytics settings and saves them to BigQuery."""

import json
import time
import os
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1alpha.types import ListPropertiesRequest
from google.analytics.admin_v1alpha.types import LinkProposalInitiatingProduct
from google.analytics.admin_v1alpha.types import LinkProposalState
from google.analytics.admin_v1alpha.types import GoogleSignalsState
from google.analytics.admin_v1alpha.types import GoogleSignalsConsent
from google.analytics.admin_v1alpha.types import DataRetentionSettings
from google.analytics.admin_v1alpha.types import IndustryCategory
from google.analytics.admin_v1alpha.types import ServiceLevel
from google.analytics.admin_v1alpha.types import DataStream
import google.auth
from google.cloud import bigquery
from google.cloud import storage
from googleapiclient.discovery import build

# Construct a BigQuery client object.
bigquery_client = bigquery.Client()

# Construct a Cloud Storage client object.
storage_client = storage.Client()

SERVICE_ACCOUNT_FILE = 'svc_key.json'
GA_MANAGEMENT_API_NAME = 'analytics'
GA_MANAGEMENT_API_VERSION = 'v3'
GA_SCOPES = [
    'https://www.googleapis.com/auth/analytics.edit',
    'https://www.googleapis.com/auth/analytics'
]
DATASET_ID = 'analytics_settings_database'
BUCKET_NAME = os.environ.get('BUCKET_NAME')

REQUEST_DELAY = .1

bucket = storage_client.get_bucket(BUCKET_NAME)


def ga_settings_download(event):
  """Retrieve GA settings and save them to a series of BigQuery Tables.

  Args:
    event (dict): Event payload.

  Returns:
    A string indicating that the settings were downloaded.
  """
  # Authenticate and construct ga service.
  analytics_apis = authorize_ga_apis()
  management_api = analytics_apis[0]
  admin_api = analytics_apis[1]

  lists = {}
  # Get account summaries.
  account_summaries = get_ua_account_summaries(management_api)
  lists['ua_account_summaries'] = account_summaries or []

  # Get goal settings.
  lists['ua_goals'] = get_ua_goals(management_api) or []

  # Get view settings.
  lists['ua_views'] = get_ua_views(management_api) or []

  # Get filter settings.
  lists['ua_filters'] = (
      get_ua_filters(management_api, account_summaries) or [])

  # Get filter link settings.
  lists['ua_filter_links'] = (
      get_ua_filter_links(management_api, account_summaries) or [])

  # Get segment settings.
  lists['ua_segments'] = get_ua_segments(management_api) or []

  # Get property level entities.
  lists |= get_ua_property_level_settings(management_api, account_summaries)

  # Get GA4 entitity settings.
  lists |= list_ga4_entities(admin_api)  

  for key in lists:
    data = lists[key]
    if data:  # If a list has data, then it will be written to BigQuery.
      create_gcs_json_file(data, key)  # Create the Cloud Storage JSON file.
      save_to_bigquery(key)  # Save the JSON file to a BigQuery table.
  return 'complete'


def authorize_ga_apis():
  """Fetches the UA Management API object.

  Returns:
    The UA Management object.
  """
  source_credentials, project_id = google.auth.default(scopes=GA_SCOPES)
  ga_management_api = build(
      GA_MANAGEMENT_API_NAME,
      GA_MANAGEMENT_API_VERSION,
      cache_discovery=False,
      credentials=source_credentials)
  ga_admin_api = AnalyticsAdminServiceClient(credentials=source_credentials)
  return (ga_management_api, ga_admin_api)


def get_ua_account_summaries(management_api):
  """Get a list of UA account summaries.

  Args:
    management_api: The Analytics Management API object.

  Returns:
    A list of the GA account summaries.
  """
  account_summaries = (
      management_api.management().accountSummaries().list().execute())
  return account_summaries['items']


def get_ua_goals(management_api):
  """Get a list of UA goal settings.

  Args:
    management_api: The Analytics Management API object.

  Returns:
    A list of UA goal settings.
  """
  goals = management_api.management().goals().list(
      accountId='~all', webPropertyId='~all', profileId='~all').execute()
  list_of_goals = goals['items']
  return list_of_goals


def get_ua_views(management_api):
  """Get a list of UA view settings.

  Args:
    management_api: The Analytics Management API object.

  Returns:
    A list of UA view settings.
  """
  views = management_api.management().profiles().list(
      accountId='~all', webPropertyId='~all').execute()
  list_of_views = views['items']
  time.sleep(REQUEST_DELAY)
  return list_of_views


def get_ua_filter_links(management_api, account_summaries):
  """Get a list of UA filter link settings for all accounts.

  Args:
    management_api: The Analytics Management API object.
    account_summaries: A list containing the account structure for all of the
      Analytics accounts the user has access to.

  Returns:
    A list of UA filter link settings for all accounts.
  """
  filter_link_list = []
  for account in account_summaries:
    filter_links = management_api.management().profileFilterLinks().list(
        accountId=account['id'], webPropertyId='~all',
        profileId='~all').execute()
    filter_link_list.extend(filter_links['items'])
    time.sleep(REQUEST_DELAY)
  return filter_link_list


def get_ua_filters(management_api, account_summaries):
  """Get a list of UA filter settings for all accounts.

  Args:
    management_api: The Analytics Management API object.
    account_summaries: A list containing the account structure for all of the
      Analytics accounts the user has access to.

  Returns:
    A list of UA filter settings for all accounts.
  """
  filter_list = []
  for account in account_summaries:
    filters = management_api.management().filters().list(
        accountId=account['id']).execute()
    filter_list.extend(filters['items'])
    time.sleep(REQUEST_DELAY)
  return filter_list


def get_ua_segments(management_api):
  """Get a list of UA segment settings to which the user has access.

  Args:
    management_api: The Analytics Management API object.

  Returns:
    A list of UA segment settings.
  """
  segment_list = []
  segments = management_api.management().segments().list().execute()
  segment_list.extend(segments['items'])
  time.sleep(REQUEST_DELAY)


def get_ua_property_level_settings(management_api, account_summaries):
  """Get a dictionary containing lists of UA property level settings.

  Args:
    management_api: The Analytics Management API object.
    account_summaries: A list containing the account structure for all of the
      Analytics accounts the user has access to.

  Returns:
    A dictionary contain separate lists for audience, Google Ads link, custom
    dimension, and custom metric settings.
  """
  lists = {
      'ua_audiences': [],
      'ua_google_ads_links': [],
      'ua_custom_dimensions': [],
      'ua_custom_metrics': [],
  }
  for account in account_summaries:
    if account['webProperties']:
      for prop in account['webProperties']:
        returned_audience_value = (
            management_api.management().remarketingAudience().list(
                accountId=account['id'], webPropertyId=prop['id']).execute())
        lists['ua_audiences'].extend(returned_audience_value['items'])
        time.sleep(REQUEST_DELAY)

        returned_al_value = (
            management_api.management().webPropertyAdWordsLinks().list(
                accountId=account['id'], webPropertyId=prop['id']).execute())
        lists['ua_google_ads_links'].extend(returned_al_value['items'])
        time.sleep(REQUEST_DELAY)

        returned_cd_value = (
            management_api.management().customDimensions().list(
                accountId=account['id'], webPropertyId=prop['id']).execute())
        lists['ua_custom_dimensions'].extend(returned_cd_value['items'])
        time.sleep(REQUEST_DELAY)

        returned_cm_value = (
            management_api.management().customMetrics().list(
                accountId=account['id'], webPropertyId=prop['id']).execute())
        lists['ua_custom_metrics'].extend(returned_cm_value['items'])
        time.sleep(REQUEST_DELAY)
  return lists


def list_ga4_entities(admin_api):
  """Get a dictionary of GA4 entity settings based on type.

  Args:
    admin_api: The Admin API object.

  Returns:
    A dictionary of GA4 entity setting lists.
  """
  entities = {
      'ga4_account_summaries': [],
      'ga4_accounts': [],
      'ga4_properties': [],
      'ga4_data_streams': [],
      'ga4_measurement_protocol_secrets': [],
      'ga4_conversion_events': [],
      'ga4_custom_dimensions': [],
      'ga4_custom_metrics': [],
      'ga4_dv360_link_proposals': [],
      'ga4_dv360_links': [],
      'ga4_firebase_links': [],
      'ga4_google_ads_links': []
  }
  for account_summary in admin_api.list_account_summaries():
    a_dict = {
        'name': account_summary.name,
        'display_name': account_summary.display_name,
        'account': account_summary.account,
        'property_summaries': []
    }
    for property_summary in account_summary.property_summaries:
      p_dict = {
          'property': property_summary.property,
          'display_name': property_summary.display_name
      }
      a_dict['property_summaries'].append(p_dict)
    entities['ga4_account_summaries'].append(a_dict)
  time.sleep(REQUEST_DELAY)
  for account in admin_api.list_accounts():
    account_dict = {
        'name': account.name,
        'display_name': account.display_name,
        'create_time': account.create_time,
        'update_time': account.update_time,
        'region_code': account.region_code,
        'deleted': account.deleted
    }
    entities['ga4_accounts'].append(account_dict)
  time.sleep(REQUEST_DELAY)
  for account_summary in entities['ga4_account_summaries']:
    prop_request = ListPropertiesRequest(
        filter=f"parent:{account_summary['account']}")
    for prop in admin_api.list_properties(prop_request):
      time.sleep(REQUEST_DELAY)
      data_retention_settings = admin_api.get_data_retention_settings(
          name=(prop.name + '/dataRetentionSettings'))
      time.sleep(REQUEST_DELAY)
      google_signals_settings = admin_api.get_google_signals_settings(
          name=(prop.name + '/googleSignalsSettings'))
      ic_enum = prop.industry_category
      sl_enum = prop.service_level
      gss_state_enum = google_signals_settings.state
      gss_consent_enum = google_signals_settings.consent
      edr_enum = data_retention_settings.event_data_retention
      prop_dict = {
          'name': prop.name,
          'create_time': prop.create_time,
          'update_time': prop.update_time,
          'parent': prop.parent,
          'display_name': prop.display_name,
          'industry_category': IndustryCategory(ic_enum).name,
          'time_zone': prop.time_zone,
          'currency_code': prop.currency_code,
          'service_level': ServiceLevel(sl_enum).name,
          'delete_time': prop.delete_time,
          'expire_time': prop.expire_time,
          'account': account_summary['account'],
          'data_sharing_settings': {
              'name': data_retention_settings.name,
              'event_data_retention': (DataRetentionSettings
                                      .RetentionDuration(edr_enum).name),
              'reset_user_data_on_new_activity':
                  data_retention_settings.reset_user_data_on_new_activity
          },
          'google_signals_settings': {
              'name': google_signals_settings.name,
              'state': GoogleSignalsState(gss_state_enum).name,
              'consent': GoogleSignalsConsent(gss_consent_enum).name
          }
      }
      entities['ga4_properties'].append(prop_dict)
    for property_summary in account_summary['property_summaries']:
      time.sleep(REQUEST_DELAY)
      for data_stream in admin_api.list_data_streams(
          parent=property_summary['property']):
        data_stream_dict = {
            'name': data_stream.name,
            'type': DataStream.DataStreamType(data_stream.type_).name,
            'display_name': data_stream.display_name,
            'create_time': data_stream.create_time,
            'update_time': data_stream.update_time,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']
        }
        if data_stream.web_stream_data != None:
          data_stream_dict['web_stream_data'] = {
            'measurment_id': data_stream.web_stream_data.measurement_id,
            'firebase_app_id': data_stream.web_stream_data.firebase_app_id,
            'default_uri': data_stream.web_stream_data.default_uri
          }
          time.sleep(REQUEST_DELAY)
          for mps in admin_api.list_measurement_protocol_secrets(
              parent=data_stream.name):
            mps_dict = {
                'name': mps.name,
                'display_name': mps.display_name,
                'secret_value': mps.secret_value,
                'stream_name': data_stream.name,
                'type': DataStream.DataStreamType(data_stream.type_).name,
                'property': property_summary['property'],
                'property_display_name': property_summary['display_name']
            }
            entities['ga4_measurement_protocol_secrets'].append(mps_dict)
        if data_stream.android_app_stream_data != None:
          data_stream_dict['android_app_stream_data'] = {
            'firebase_app_id': (data_stream
                               .android_app_stream_data.firebase_app_id),
            'package_name': data_stream.android_app_stream_data.package_name
          }
          time.sleep(REQUEST_DELAY)
          for mps in admin_api.list_measurement_protocol_secrets(
              parent=data_stream.name):
            mps_dict = {
                'name': mps.name,
                'display_name': mps.display_name,
                'secret_value': mps.secret_value,
                'stream_name': data_stream.name,
                'type': DataStream.DataStreamType(data_stream.type_).name,
                'property': property_summary['property'],
                'property_display_name': property_summary['display_name']
            }
            entities['ga4_measurement_protocol_secrets'].append(mps_dict)
        if data_stream.ios_app_stream_data != None:
          data_stream_dict['ios_app_stream_data'] = {
            'firebase_app_id': data_stream.ios_app_stream_data.firebase_app_id,
            'bundle_id': data_stream.ios_app_stream_data.bundle_id
          }
          time.sleep(REQUEST_DELAY)
          for mps in admin_api.list_measurement_protocol_secrets(
              parent=data_stream.name):
            mps_dict = {
                'name': mps.name,
                'display_name': mps.display_name,
                'secret_value': mps.secret_value,
                'stream_name': data_stream.name,
                'type': DataStream.DataStreamType(data_stream.type_).name,
                'property': property_summary['property'],
                'property_display_name': property_summary['display_name']
            }
            entities['ga4_measurement_protocol_secrets'].append(mps_dict)
        entities['ga4_data_streams'].append(data_stream_dict)
      time.sleep(REQUEST_DELAY)
      for event in admin_api.list_conversion_events(
          parent=property_summary['property']):
        event_dict = {
            'name': event.name,
            'event_name': event.event_name,
            'create_time': event.create_time,
            'deletable': event.deletable,
            'custom': event.custom,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']
        }
        entities['ga4_conversion_events'].append(event_dict)
      time.sleep(REQUEST_DELAY)
      for cd in admin_api.list_custom_dimensions(
          parent=property_summary['property']):
        cd_dict = {
            'name': cd.name,
            'parameter_name': cd.parameter_name,
            'display_name': cd.display_name,
            'description': cd.description,
            'scope': cd.scope,
            'disallow_ads_personalization': cd.disallow_ads_personalization,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']
        }
        entities['ga4_custom_dimensions'].append(cd_dict)
      time.sleep(REQUEST_DELAY)
      for cm in admin_api.list_custom_metrics(
          parent=property_summary['property']):
        cm_dict = {
            'name': cm.name,
            'parameter_name': cm.parameter_name,
            'display_name': cm.display_name,
            'description': cm.description,
            'scope': cm.scope,
            'measurement_unit': cm.measurement_unit,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']
        }
        entities['ga4_custom_metrics'].append(cm_dict)
      time.sleep(REQUEST_DELAY)
      for link in admin_api.list_google_ads_links(
          parent=property_summary['property']):
        link_dict = {
            'name': link.name,
            'customer_id': link.customer_id,
            'can_manage_clients': link.can_manage_clients,
            'ads_personalization_enabled': link.ads_personalization_enabled,
            'create_time': link.create_time,
            'update_time': link.update_time,
            'creator_email_address': link.creator_email_address,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']
        }
        entities['ga4_google_ads_links'].append(link_dict)
      time.sleep(REQUEST_DELAY)
      for link in admin_api.list_firebase_links(
          parent=property_summary['property']):
        link_dict = {
            'name': link.name,
            'project': link.project,
            'create_time': link.create_time,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']
        }
        entities['ga4_firebase_links'].append(link_dict)
      time.sleep(REQUEST_DELAY)
      for link in admin_api.list_display_video360_advertiser_links(
          parent=property_summary['property']):
        link_dict = {
            'name': link.name,
            'advertiser_id': link.advertiser_id,
            'advertiser_display_name': link.advertiser_display_name,
            'ads_personalization_enabled': link.ads_personalization_enabled,
            'campaign_data_sharing_enabled': link.campaign_data_sharing_enabled,
            'cost_data_sharing_enabled': link.cost_data_sharing_enabled,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']
        }
        entities['ga4_dv360_links'].append(link_dict)
      time.sleep(REQUEST_DELAY)
      for proposal in (
          admin_api.list_display_video360_advertiser_link_proposals(
              parent=property_summary['property'])):
        lpip_enum = (proposal.link_proposal_status_details
                                  .link_proposal_initiating_product)
        lps_enum = (proposal.link_proposal_status_details
                                   .link_proposal_state)
        proposals_dict = {
            'name':
                proposal.name,
            'advertiser_id':
                proposal.adveriser_id,
            'link_proposal_status_details': {
                'link_proposal_initiating_product':
                    LinkProposalInitiatingProduct(lpip_enum).name,
                'requestor_email':
                    proposal.link_proposal_status_details.requestor_email,
                'link_proposal_state': LinkProposalState(lps_enum).name
            },
            'advertiser_display_name':
                proposal.advertiser_display_name,
            'validation_email':
                proposal.validation_email,
            'ads_personalization_enabled':
                proposal.ads_personalization_enabled,
            'campaign_data_sharing_enabled':
                proposal.campaign_data_sharing_enabled,
            'cost_data_sharing_enabled':
                proposal.cost_data_sharing_enabled,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']
        }
        entities['ga4_dv360_link_proposals'].append(proposal_dict)
  return entities


def create_gcs_json_file(data, file_name):
  """Creates a Cloud Storage JSON file containing the data for a setting type.

  Args:
    data: The list to be written to the Cloud Storage JSON file.
    file_name: The name for the Cloud Storage JSON file.
  """
  data_blob = bucket.blob(file_name)
  with open(f'/tmp/{file_name}', 'w') as w:
    for item in data:
      json.dump(item, w, default=str)
      w.write('\n')
    w.close()
    with open(f'/tmp/{file_name}', 'r') as r:
      data_blob.upload_from_file(r)
      r.close()


def save_to_bigquery(table_id):
  """Saves data to bigquery.

  Args:
    table_id: The table id and Cloud Storage file name.
  """
  job_config = bigquery.LoadJobConfig()
  job_config.autodetect = True
  job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
  dataset_ref = bigquery_client.dataset(DATASET_ID)
  uri = f'gs://{BUCKET_NAME}/{table_id}'
  table = dataset_ref.table(table_id)
  load_job = bigquery_client.load_table_from_uri(
      uri, table, job_config=job_config)
  load_job.result()  # Waits for table load to complete.
