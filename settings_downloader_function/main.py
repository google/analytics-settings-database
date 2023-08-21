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
import humps
import google.auth
from google.analytics.admin import AnalyticsAdminServiceClient
from google.analytics.admin_v1alpha.types import ListPropertiesRequest
from google.protobuf.json_format import MessageToDict
from google.cloud import bigquery
from googleapiclient.discovery import build

# Construct a BigQuery client object.
bigquery_client = bigquery.Client()

SERVICE_ACCOUNT_FILE = 'svc_key.json'
GA_MANAGEMENT_API_NAME = 'analytics'
GA_MANAGEMENT_API_VERSION = 'v3'
GA_SCOPES = [
    'https://www.googleapis.com/auth/analytics.edit',
    'https://www.googleapis.com/auth/analytics'
]
DATASET_ID = 'analytics_settings_database'

REQUEST_DELAY = .1

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
      errors = bigquery_client.insert_rows_json(f'{DATASET_ID}.{key}', data)
      if errors != []:
        f_errors = format(errors)
        print(f'{key}: Encountered errors while inserting rows: {f_errors}')
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
      'ga4_google_ads_links': [],
      'ga4_audiences': []
  }
  # Account summaries
  for account_summary in admin_api.list_account_summaries():
    summaries_dict = humps.decamelize(MessageToDict(account_summary._pb))
    entities['ga4_account_summaries'].append(summaries_dict)
  time.sleep(REQUEST_DELAY)
  # Accounts
  for account in admin_api.list_accounts():
    account_dict = humps.decamelize(MessageToDict(account._pb))
    entities['ga4_accounts'].append(account_dict)
  time.sleep(REQUEST_DELAY)
  # Properties, data retention settings, and Google Signals settings
  for account_summary in entities['ga4_account_summaries']:
    prop_request = ListPropertiesRequest(
        filter=f"ancestor:{account_summary['account']}")
    # Properties
    for prop in admin_api.list_properties(prop_request):
      property_dict = humps.decamelize(MessageToDict(prop._pb))
      time.sleep(REQUEST_DELAY)
      # Set data retention settings for each property
      data_retention_settings = admin_api.get_data_retention_settings(
          name=(prop.name + '/dataRetentionSettings'))
      data_retention_dict = humps.decamelize(
        MessageToDict(data_retention_settings._pb))
      property_dict['data_sharing_settings'] = data_retention_dict
      time.sleep(REQUEST_DELAY)
      # Set Google Signals settings for each property
      google_signals_settings = admin_api.get_google_signals_settings(
          name=(prop.name + '/googleSignalsSettings'))
      google_signals_dict = humps.decamelize(
        MessageToDict(google_signals_settings._pb))
      property_dict['google_signals_settings'] = google_signals_dict
      time.sleep(REQUEST_DELAY)
      # Set Attribution settings for each property
      attribution_settings = admin_api.get_attribution_settings(
          name=(prop.name + '/attributionSettings'))
      attribution_dict = humps.decamelize(
        MessageToDict(attribution_settings._pb))
      property_dict['attribution_settings'] = attribution_dict
      # Append property to list of properties
      entities['ga4_properties'].append(property_dict)
    time.sleep(REQUEST_DELAY)
    try:
      # Property level settings
      for property_summary in account_summary['property_summaries']:
        if property_summary == None:
          #If no property, skip row
          continue
        property_path = property_summary['property']
        property_display_name = property_summary['display_name']
        # Data streams
        try:
          for data_stream in admin_api.list_data_streams(
              parent=property_path):
            data_stream_dict = format_resource_dict(
              data_stream, property_path, property_display_name)
            time.sleep(REQUEST_DELAY)
            if data_stream.web_stream_data != None:
              # Web stream measurement protocol secrets
              for mps in admin_api.list_measurement_protocol_secrets(
                  parent=data_stream.name):
                mps_dict = format_resource_dict(
                  data_stream, property_path, property_display_name)
                mps_dict['type'] = DataStream.DataStreamType(data_stream.type_).name
                mps_dict['stream_name'] = data_stream.name
                entities['ga4_measurement_protocol_secrets'].append(mps_dict)
              time.sleep(REQUEST_DELAY)
            if data_stream.android_app_stream_data != None:
              # Android app data stream measurement protocol secrets
              for mps in admin_api.list_measurement_protocol_secrets(
                  parent=data_stream.name):
                mps_dict = format_resource_dict(
                  data_stream, property_path, property_display_name)
                mps_dict['type'] = DataStream.DataStreamType(data_stream.type_).name
                mps_dict['stream_name'] = data_stream.name
                entities['ga4_measurement_protocol_secrets'].append(mps_dict)
              time.sleep(REQUEST_DELAY)
            if data_stream.ios_app_stream_data != None:
              # iOS app data strem measurement protocol secrets
              for mps in admin_api.list_measurement_protocol_secrets(
                  parent=data_stream.name):
                mps_dict = format_resource_dict(
                  data_stream, property_path, property_display_name)
                mps_dict['type'] = DataStream.DataStreamType(data_stream.type_).name
                mps_dict['stream_name'] = data_stream.name
                entities['ga4_measurement_protocol_secrets'].append(mps_dict)
            entities['ga4_data_streams'].append(data_stream_dict)
        except:
          continue
        time.sleep(REQUEST_DELAY)
        # Events
        for event in admin_api.list_conversion_events(
            parent=property_path):
          formatted_dict = format_resource_dict(
            event, property_path, property_display_name)
          entities['ga4_conversion_events'].append(formatted_dict)
        time.sleep(REQUEST_DELAY)
        # Custom dimensions
        for cd in admin_api.list_custom_dimensions(
            parent=property_path):
          formatted_dict = format_resource_dict(
            cd, property_path, property_display_name)
          entities['ga4_custom_dimensions'].append(formatted_dict)
        time.sleep(REQUEST_DELAY)
        # Custom metrics
        for cm in admin_api.list_custom_metrics(
            parent=property_path):
          formatted_dict = format_resource_dict(
            cm, property_path, property_display_name)
          entities['ga4_custom_metrics'].append(formatted_dict)
        time.sleep(REQUEST_DELAY)
        # Google ads links
        for link in admin_api.list_google_ads_links(
            parent=property_path):
          formatted_dict = format_resource_dict(
            link, property_path, property_display_name)
          entities['ga4_google_ads_links'].append(formatted_dict)
        time.sleep(REQUEST_DELAY)
        # Firebase links
        for link in admin_api.list_firebase_links(
            parent=property_path):
          formatted_dict = format_resource_dict(
            link, property_path, property_display_name)
          entities['ga4_firebase_links'].append(formatted_dict)
        time.sleep(REQUEST_DELAY)
        # DV360 advertiser links
        for link in admin_api.list_display_video360_advertiser_links(
            parent=property_path):
          formatted_dict = format_resource_dict(
            link, property_path, property_display_name)
          entities['ga4_dv360_links'].append(formatted_dict)
        time.sleep(REQUEST_DELAY)
        # DV360 advertiser link proposals
        for proposal in (
            admin_api.list_display_video360_advertiser_link_proposals(
                parent=property_path)):
          formatted_dict = format_resource_dict(
            proposal, property_path, property_display_name)
          entities['ga4_dv360_link_proposals'].append(formatted_dict)
        time.sleep(REQUEST_DELAY)
        # Audiences 
        for audience in (admin_api.list_audiences(parent=property_path)):
          formatted_dict = format_resource_dict(
            audience, property_path, property_display_name)
          if 'filter_clauses' in formatted_dict:
            string_clauses = json.dumps(formatted_dict['filter_clauses'])
            formatted_dict['filter_clauses'] = string_clauses
          entities['ga4_audiences'].append(formatted_dict)
        time.sleep(REQUEST_DELAY)
    except:
      continue
  return entities
  
def format_resource_dict(data, property_path, property_display_name):
  data_dict = humps.decamelize(MessageToDict(data._pb))
  data_dict['property'] = property_path
  data_dict['property_display_name'] = property_display_name
  return data_dict