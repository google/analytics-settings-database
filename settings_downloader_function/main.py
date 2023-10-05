# Copyright 2023 Google LLC
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
from google.analytics.admin_v1alpha.types import DataStream
from google.protobuf.json_format import MessageToDict
from google.cloud import bigquery

# Construct a BigQuery client object.
bigquery_client = bigquery.Client()

SERVICE_ACCOUNT_FILE = 'credentials.json'
GA_SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DATASET_ID = 'analytics_settings_database'

REQUEST_DELAY = .2

def ga_settings_download(event):
  """Retrieve GA settings and save them to a series of BigQuery Tables.

  Args:
    event (dict): Event payload.

  Returns:
    A string indicating that the settings were downloaded.
  """
  # Authenticate and construct ga service.
  admin_api = authorize_ga_apis()

  # Get GA4 entitity settings.
  lists = {}
  lists |= list_ga4_resources(admin_api)  

  dataset = bigquery_client.dataset(DATASET_ID)
  for key in lists:
    data = lists[key]
    if data:
      # Create newline delimited JSON file to to loaded into BigQuery.
      with open('/tmp/data.json', 'w') as json_file:
        for resource in data:
          json.dump(resource, json_file)
          json_file.write('\n')
      json_file.close()
      r = open('/tmp/data.json', 'rb')
      # Create load job.
      job_config = bigquery.LoadJobConfig()
      job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
      table = dataset.table(key)
      job = bigquery_client.load_table_from_file(
        r, table, job_config=job_config)
      # Load the data into BigQuery.
      result = job.result()
      print(result)
      # Delete the temporary file.
      os.remove('/tmp/data.json')
  print('GA settings import complete')
  return 'success'

def authorize_ga_apis():
  """Fetches the Google Analytics Admin API client.

  Returns:
    The admin API client.
  """
  source_credentials, project_id = google.auth.default(scopes=GA_SCOPES)
  ga_admin_api = AnalyticsAdminServiceClient(credentials=source_credentials)
  return ga_admin_api

def list_ga4_resources(admin_api):
  """Get a dictionary of GA4 entity settings based on type.

  Args:
    admin_api: The Admin API client.

  Returns:
    A dictionary of GA4 resource lists.
  """
  resources = {
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
      'ga4_audiences': [],
      'ga4_enhanced_measurement_settings': [],
      'ga4_sa360_links': [],
      'ga4_bigquery_links': [],
      'ga4_expanded_data_sets': [],
      'ga4_channel_groups': [],
      'ga4_adsense_links': [],
      'ga4_event_create_rules': [],
      'ga4_sk_ad_network_conversion_value_schemas': []
  }
  # Account summaries
  for account_summary in admin_api.list_account_summaries(
    request={'page_size': 200}):
    summaries_dict = humps.decamelize(MessageToDict(account_summary._pb))
    resources['ga4_account_summaries'].append(summaries_dict)
  time.sleep(REQUEST_DELAY)
  # Accounts
  for account in admin_api.list_accounts(request={'page_size': 200}):
    account_dict = humps.decamelize(MessageToDict(account._pb))
    resources['ga4_accounts'].append(account_dict)
  time.sleep(REQUEST_DELAY)
  # Properties
  for account_summary in resources['ga4_account_summaries']:
    prop_request = ListPropertiesRequest(
        filter=f"ancestor:{account_summary['account']}",
        page_size=200)
    # Settings specific to properties
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
      resources['ga4_properties'].append(property_dict)
    time.sleep(REQUEST_DELAY)
    # Settings below the property level
    for property_summary in account_summary['property_summaries']:
      property_path = property_summary['property']
      property_display_name = property_summary['display_name']
      # Data streams
      for data_stream in admin_api.list_data_streams(
          request={'parent': property_path, 'page_size': 200}):
        data_stream_dict = format_resource_dict(
          data_stream, property_path, property_display_name)
        time.sleep(REQUEST_DELAY)
        # Measurement protocol secrets
        for mps in admin_api.list_measurement_protocol_secrets(
            parent=data_stream.name):
          mps_dict = format_resource_dict(
            mps, property_path, property_display_name)
          mps_dict['type'] = DataStream.DataStreamType(data_stream.type_).name
          mps_dict['stream_name'] = data_stream.name
          resources['ga4_measurement_protocol_secrets'].append(mps_dict)
          time.sleep(REQUEST_DELAY)
        # Event Create Rules
        for ecr in admin_api.list_event_create_rules(
        request={'page_size': 200, 'parent': data_stream.name}):
          resource_dict = format_resource_dict(
            resource, property_path, property_display_name)
          resource_dict['type'] = DataStream.DataStreamType(data_stream.type_).name
          resource_dict['stream_name'] = data_stream.name
          resources['ga4_event_create_rules'].append(resource_dict)
          time.sleep(REQUEST_DELAY)
        if DataStream.DataStreamType(data_stream.type_).name == 'WEB_DATA_STREAM':
          # Enhanced measurement settings
          ems = admin_api.get_enhanced_measurement_settings(
            name=data_stream.name + '/enhancedMeasurementSettings')
          ems_dict = format_resource_dict(
            ems, property_path, property_display_name)
          ems_dict['stream_name'] = data_stream.name
          resources['ga4_enhanced_measurement_settings'].append(ems_dict)
          time.sleep(REQUEST_DELAY)
        if DataStream.DataStreamType(data_stream.type_).name == 'IOS_APP_DATA_STREAM':
          # SK Ad Network Conversion Value Schema settings
          for resource in admin_api.list_sk_ad_network_conversion_value_schemas(
            request={'page_size': 200, 'parent': data_stream.name}):
            resource_dict = format_resource_dict(
              resource, property_path, property_display_name)
            resource_dict['stream_name'] = data_stream.name
            resources['ga4_sk_ad_network_conversion_value_schemas'].append(
              resource_dict)
            time.sleep(REQUEST_DELAY)
      # Events
      for event in admin_api.list_conversion_events(
          request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          event, property_path, property_display_name)
        resources['ga4_conversion_events'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # Custom dimensions
      for cd in admin_api.list_custom_dimensions(
          request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          cd, property_path, property_display_name)
        resources['ga4_custom_dimensions'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # Custom metrics
      for cm in admin_api.list_custom_metrics(
          request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          cm, property_path, property_display_name)
        resources['ga4_custom_metrics'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # Google ads links
      for link in admin_api.list_google_ads_links(
          request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          link, property_path, property_display_name)
        resources['ga4_google_ads_links'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # Firebase links
      for link in admin_api.list_firebase_links(
          request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          link, property_path, property_display_name)
        resources['ga4_firebase_links'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # DV360 advertiser links
      for link in admin_api.list_display_video360_advertiser_links(
          request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          link, property_path, property_display_name)
        resources['ga4_dv360_links'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # DV360 advertiser link proposals
      for proposal in (
          admin_api.list_display_video360_advertiser_link_proposals(
              request={'parent': property_path, 'page_size': 200})):
        formatted_dict = format_resource_dict(
          proposal, property_path, property_display_name)
        resources['ga4_dv360_link_proposals'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # SA360 advertiser links
      for link in admin_api.list_search_ads360_links(
          request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          link, property_path, property_display_name)
        resources['ga4_sa360_links'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # BigQuery links
      for link in admin_api.list_big_query_links(
          request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          link, property_path, property_display_name)
        resources['ga4_bigquery_links'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # Audiences 
      for audience in admin_api.list_audiences(
      request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          audience, property_path, property_display_name)
        if 'filter_clauses' in formatted_dict:
          string_clauses = json.dumps(formatted_dict['filter_clauses'])
          formatted_dict['filter_clauses'] = string_clauses
        resources['ga4_audiences'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # Expanded Data Sets 
      for data_set in admin_api.list_expanded_data_sets(
      request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          data_set, property_path, property_display_name)
        if 'dimension_ilter_expression' in formatted_dict:
          string_clauses = json.dumps(
            formatted_dict['dimension_ilter_expression'])
          formatted_dict['dimension_ilter_expression'] = string_clauses
        resources['ga4_expanded_data_sets'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # Custom Channel Groups
      for resource in admin_api.list_channel_groups(
      request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
          resource, property_path, property_display_name)
        if 'grouping_rule' in formatted_dict:
          string_clauses = json.dumps(formatted_dict['grouping_rule'])
          formatted_dict['grouping_rule'] = string_clauses
        resources['ga4_channel_groups'].append(formatted_dict)
      time.sleep(REQUEST_DELAY)
      # AdSense Links
      for resource in admin_api.list_ad_sense_links(
      request={'parent': property_path, 'page_size': 200}):
        formatted_dict = format_resource_dict(
        resource, property_path, property_display_name)
        resources['ga4_adsense_links'].append(formatted_dict)
        time.sleep(REQUEST_DELAY)
  return resources
  
def format_resource_dict(data, property_path, property_display_name):
  data_dict = humps.decamelize(MessageToDict(data._pb))
  data_dict['property'] = property_path
  data_dict['property_display_name'] = property_display_name
  return data_dict
