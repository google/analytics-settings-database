# Copyright 2021 Google LLC
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
  property_entities = get_ua_property_level_settings(
      management_api, account_summaries)
  lists['ua_custom_dimensions'] = property_entities['cds'] or []
  lists['ua_custom_metrics'] = property_entities['cms'] or []
  lists['ua_audiences'] = property_entities['audiences'] or []
  lists['ua_google_ads_links'] = property_entities['google_ads_links'] or []

  # Get GA4 entitity settings.
  ga4_entities = list_ga4_entities(admin_api)
  lists['ga4_account_summaries'] = ga4_entities['summaries'] or []
  lists['ga4_accounts'] = ga4_entities['accounts'] or []
  lists['ga4_properties'] = ga4_entities['properties'] or []
  lists['ga4_android_app_data_streams'] = ga4_entities[
      'android_app_data_streams'] or []
  lists['ga4_measurement_protocol_secrets'] = ga4_entities[
      'measurement_protocol_secrets'] or []
  lists['ga4_conversion_events'] = ga4_entities['conversion_events'] or []
  lists['ga4_custom_dimensions'] = ga4_entities['custom_dimensions'] or []
  lists['ga4_custom_metrics'] = ga4_entities['custom_metrics'] or []
  #  lists['ga4_dv360_link_proposals'] = ga4_entities['dv360_link_proposals'] or []
  #  lists['ga4_dv360_links'] = ga4_entities['dv360_links'] or []
  lists['ga4_firebase_links'] = ga4_entities['firebase_links'] or []
  lists['ga4_google_ads_links'] = ga4_entities['google_ads_links'] or []
  lists['ga4_ios_app_data_streams'] = ga4_entities['ios_app_data_streams'] or []
  lists['ga4_web_data_streams'] = ga4_entities['web_data_streams'] or []

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
      'audiences': [],
      'google_ads_links': [],
      'cds': [],
      'cms': [],
  }
  for account in account_summaries:
    if account['webProperties']:
      for prop in account['webProperties']:
        returned_audience_value = (
            management_api.management().remarketingAudience().list(
                accountId=account['id'], webPropertyId=prop['id']).execute())
        lists['audiences'].extend(returned_audience_value['items'])
        time.sleep(REQUEST_DELAY)

        returned_al_value = (
            management_api.management().webPropertyAdWordsLinks().list(
                accountId=account['id'], webPropertyId=prop['id']).execute())
        lists['google_ads_links'].extend(returned_al_value['items'])
        time.sleep(REQUEST_DELAY)

        returned_cd_value = (
            management_api.management().customDimensions().list(
                accountId=account['id'], webPropertyId=prop['id']).execute())
        lists['cds'].extend(returned_cd_value['items'])
        time.sleep(REQUEST_DELAY)

        returned_cm_value = (
            management_api.management().customMetrics().list(
                accountId=account['id'], webPropertyId=prop['id']).execute())
        lists['cms'].extend(returned_cm_value['items'])
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
      'summaries': [],
      'accounts': [],
      'properties': [],
      'android_app_data_streams': [],
      'measurement_protocol_secrets': [],
      'conversion_events': [],
      'custom_dimensions': [],
      'custom_metrics': [],
      'dv360_link_proposals': [],
      'dv360_links': [],
      'firebase_links': [],
      'google_ads_links': [],
      'ios_app_data_streams': [],
      'web_data_streams': []
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
    entities['summaries'].append(a_dict)
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
    entities['accounts'].append(account_dict)
  time.sleep(REQUEST_DELAY)
  for account_summary in entities['summaries']:
    prop_request = ListPropertiesRequest(
        filter=f"parent:{account_summary['account']}")
    for prop in admin_api.list_properties(prop_request):
      time.sleep(REQUEST_DELAY)
      data_retention_settings = admin_api.get_data_retention_settings(
          name=(prop.name + '/dataRetentionSettings'))
      time.sleep(REQUEST_DELAY)
      google_signals_settings = admin_api.get_google_signals_settings(
          name=(prop.name + '/googleSignalsSettings'))
      prop_dict = {
          'name': prop.name,
          'create_time': prop.create_time,
          'update_time': prop.update_time,
          'parent': prop.parent,
          'display_name': prop.display_name,
          'industry_category': prop.industry_category,
          'time_zone': prop.time_zone,
          'currency_code': prop.currency_code,
          'service_level': prop.service_level,
          'delete_time': prop.delete_time,
          'expire_time': prop.expire_time,
          'account': account_summary['account'],
          'data_sharing_settings': {
              'name':
                  data_retention_settings.name,
              'event_data_retention':
                  data_retention_settings.event_data_retention,
              'reset_user_data_on_new_activity':
                  data_retention_settings.reset_user_data_on_new_activity
          },
          'google_signals_settings': {
              'name': google_signals_settings.name,
              'state': google_signals_settings.state,
              'consent': google_signals_settings.consent
          }
      }
      entities['properties'].append(prop_dict)
    for property_summary in account_summary['property_summaries']:
      time.sleep(REQUEST_DELAY)
      for android_data_stream in admin_api.list_android_app_data_streams(
          parent=property_summary['property']):
        android_data_stream_dict = {
            'name': android_data_stream.name,
            'firebase_app_id': android_data_stream.firebase_app_id,
            'create_time': android_data_stream.create_time,
            'update_time': android_data_stream.update_time,
            'package_name': android_data_stream.package_name,
            'display_name': android_data_stream.display_name,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']
        }
        entities['android_app_data_streams'].append(android_data_stream_dict)
        time.sleep(REQUEST_DELAY)
        for mps in admin_api.list_measurement_protocol_secrets(
            parent=android_data_stream_dict['name']):
          mps_dict = {
              'name': mps.name,
              'display_name': mps.display_name,
              'secret_value': mps.secret_value,
              'stream_name': android_data_stream_dict['name'],
              'type': 'android app',
              'property': property_summary['property'],
              'property_display_name': property_summary['display_name']
          }
          entities['measurement_protocol_secrets'].append(mps_dict)
      time.sleep(REQUEST_DELAY)
      for ios_data_stream in admin_api.list_ios_app_data_streams(
          parent=property_summary['property']):
        ios_data_stream_dict = {
            'name': ios_data_stream.name,
            'firebase_app_id': ios_data_stream.firebase_app_id,
            'create_time': ios_data_stream.create_time,
            'update_time': ios_data_stream.update_time,
            'bundle_id': ios_data_stream.bundle_id,
            'display_name': ios_data_stream.display_name,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']
        }
        entities['ios_app_data_streams'].append(ios_data_stream_dict)
        time.sleep(REQUEST_DELAY)
        for mps in admin_api.list_measurement_protocol_secrets(
            parent=ios_data_stream_dict['name']):
          mps_dict = {
              'name': mps.name,
              'display_name': mps.display_name,
              'secret_value': mps.secret_value,
              'stream_name': ios_data_stream_dict['name'],
              'type': 'ios app',
              'property': property_summary['property'],
              'property_display_name': property_summary['display_name']
          }
          entities['measurement_protocol_secrets'].append(mps_dict)
      time.sleep(REQUEST_DELAY)
      for web_data_stream in admin_api.list_web_data_streams(
          parent=property_summary['property']):
#        time.sleep(REQUEST_DELAY)
#        enhanced_settings = admin_api.get_enhanced_measurement_settngs(
#            name=(web_data_stream.name + '/enhancedMeasurementSettings'))
        web_data_stream_dict = {
            'name': web_data_stream.name,
            'firebase_app_id': web_data_stream.firebase_app_id,
            'create_time': web_data_stream.create_time,
            'update_time': web_data_stream.update_time,
            'measurement_id': web_data_stream.measurement_id,
            'display_name': web_data_stream.display_name,
            'default_uri': web_data_stream.default_uri,
            'property': property_summary['property'],
            'property_display_name': property_summary['display_name']#,
#            'enhanced_measurement_settings': {
#                'name':
#                    enhanced_settings.name,
#                'stream_enabled':
#                    enhanced_settings.stream_enabled,
#                'page_views_enabled':
#                    enhanced_settings.page_views_enabled,
#                'scrolls_enabled':
#                    enhanced_settings.scrolls_enabled,
#                'outbound_link_clicks_enabled':
#                    enhanced_settings.outbound_link_clicks_enabled,
#                'site_seearch_eenabled':
#                    enhanced_settings.site_seearch_eenabled,
#                'video_engagement_enabled':
#                    enhanced_settings.video_engagement_enabled,
#                'file_downloads_enabled':
#                    enhanced_settings.file_downloads_enabled,
#                'page_loads_enabled':
#                    enhanced_settings.page_loads_enabled,
#                'page_changes_enabled':
#                    enhanced_settings.page_changes_enabled,
#                'search_query_parameter':
#                    enhanced_settings.search_query_parameter,
#                'uri_query_parameter':
#                    enhanced_settings.uri_query_parameter
#            }
        }
        entities['web_data_streams'].append(web_data_stream_dict)
        time.sleep(REQUEST_DELAY)
        for mps in admin_api.list_measurement_protocol_secrets(
            parent=web_data_stream_dict['name']):
          mps_dict = {
              'name': mps.name,
              'display_name': mps.display_name,
              'secret_value': mps.secret_value,
              'stream_name': web_data_stream_dict['name'],
              'type': 'web',
              'property': property_summary['property'],
              'property_display_name': property_summary['display_name']
          }
          entities['measurement_protocol_secrets'].append(mps_dict)
          time.sleep(REQUEST_DELAY)
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
        entities['conversion_events'].append(event_dict)
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
        entities['custom_dimensions'].append(cd_dict)
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
        entities['custom_metrics'].append(cm_dict)
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
        entities['google_ads_links'].append(link_dict)
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
        entities['firebase_links'].append(link_dict)
#      time.sleep(REQUEST_DELAY)
#      for link in admin_api.list_display_video_360_advertiser_links(
#          parent=property_summary['property']):
#        link_dict = {
#            'name': link.name,
#            'advertiser_id': link.advertiser_id,
#            'advertiser_display_name': link.advertiser_display_name,
#            'ads_personalization_enabled': link.ads_personalization_enabled,
#            'campaign_data_sharing_enabled': link.campaign_data_sharing_enabled,
#            'cost_data_sharing_enabled': link.cost_data_sharing_enabled,
#            'property': property_summary['property'],
#            'property_display_name': property_summary['display_name']
#        }
#        entities['dv360_links'].append(link_dict)
#      time.sleep(REQUEST_DELAY)
#      for proposal in (
#          admin_api.list_display_video_360_advertiser_link_proposals(
#              parent=property_summary['property'])):
#        proposal_dict = {
#            'name':
#                proposal.name,
#            'advertiser_id':
#                proposal.adveriser_id,
#            'link_proposal_status_details': {
#                'link_proposal_initiating_product':
#                    proposal.link_proposal_status_details
#                    .link_proposal_initiating_product,
#                'requestor_email':
#                    proposal.link_proposal_status_details.requestor_email,
#                'link_proposal_state':
#                    proposal.link_proposal_status_details.link_proposal_state
#            },
#            'advertiser_display_name':
#                proposal.advertiser_display_name,
#            'validation_email':
#                proposal.validation_email,
#            'ads_personalization_enabled':
#                proposal.ads_personalization_enabled,
#            'campaign_data_sharing_enabled':
#                proposal.campaign_data_sharing_enabled,
#            'cost_data_sharing_enabled':
#                proposal.cost_data_sharing_enabled
#        }
#        entities['dv360_link_proposals'].append(proposal_dict)
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
