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

import os
import functions_framework
from google.cloud import bigquery
from google.cloud.bigquery.table import TableReference

PROJECT_ID = os.environ.get('PROJECT_ID')
DATASET_ID = os.environ.get('DATASET_ID')
TABLE_ID = os.environ.get('TABLE_ID')

# Construct a BigQuery client object.
client = bigquery.Client()

@functions_framework.http
def main(request):
    query = """
        SELECT
        summaries.display_name AS account_name,
        summaries.account AS account_id,
        property_summaries.display_name AS property_name,
        property_summaries.property AS property_id,
        properties.service_level as standard_or_360,
        properties.google_signals_state as google_signals_state,
        SUM(DISTINCT CASE WHEN data_streams.type = 'WEB_DATA_STREAM' THEN 1 ELSE 0 END) as number_of_web_streams,
        SUM(DISTINCT CASE WHEN data_streams.type = 'ANDROID_APP_DATA_STREAM' THEN 1 ELSE 0 END) as number_of_android_app_streams,
        SUM(DISTINCT CASE WHEN data_streams.type = 'IOS_APP_DATA_STREAM' THEN 1 ELSE 0 END) as number_of_ios_app_streams,
        (CASE WHEN SUM(DISTINCT CASE WHEN data_streams.type = 'WEB_DATA_STREAM' THEN 1 ELSE 0 END) > 0 THEN TRUE ELSE FALSE END) AS web_streams_created,
        (CASE WHEN SUM(DISTINCT CASE WHEN data_streams.type = 'ANDROID_APP_DATA_STREAM' THEN 1 ELSE 0 END) > 0 THEN TRUE ELSE FALSE END) AS android_streams_created,
        (CASE WHEN SUM(DISTINCT CASE WHEN data_streams.type = 'IOS_APP_DATA_STREAM' THEN 1 ELSE 0 END) > 0 THEN TRUE ELSE FALSE END) AS ios_streams_created,
        (CASE WHEN SUM(conversion_events.is_custom) > 0 THEN TRUE ELSE FALSE END) AS custom_conversions_created,
        SUM(conversion_events.is_custom) AS number_of_custom_conversion_events,
        (CASE WHEN COUNT(DISTINCT google_ads_links.customer_id) > 0 THEN TRUE ELSE FALSE END) AS google_ads_linked,
        COUNT(DISTINCT google_ads_links.customer_id) AS number_of_google_ads_links,
        (CASE WHEN COUNT(DISTINCT dv360_links.name) > 0 THEN TRUE ELSE FALSE END) AS dv360_linked,
        COUNT(DISTINCT dv360_links.name) AS number_of_dv360_links,
        (CASE WHEN COUNT(DISTINCT firebase_links.project) > 0 THEN TRUE ELSE FALSE END) AS firebase_linked,
        COUNT(DISTINCT firebase_links.project) AS number_of_firebase_links,
        (CASE WHEN COUNT(DISTINCT custom_dimensions.parameter_name) > 0 THEN TRUE ELSE FALSE END) AS custom_dimensions_created,
        COUNT(DISTINCT custom_dimensions.parameter_name) AS number_of_custom_dimensions,
        (CASE WHEN COUNT(DISTINCT custom_metrics.parameter_name) > 0 THEN TRUE ELSE FALSE END) AS custom_metrics_created,
        COUNT(DISTINCT custom_metrics.parameter_name) AS number_of_custom_metrics,
        (CASE WHEN COUNT(DISTINCT measurement_protocol_secrets.secret_value) > 0 THEN TRUE ELSE FALSE END) AS measurement_protocol_secret_created,
        COUNT(DISTINCT measurement_protocol_secrets.secret_value) AS number_of_measurement_protocol_secrets,
        (CASE WHEN SUM(audiences.is_custom) > 0 THEN TRUE ELSE FALSE END) AS custom_audiences_created,
        SUM(audiences.is_custom) AS number_of_custom_audiences
        FROM
        analytics_settings_database.ga4_account_summaries AS summaries,
        UNNEST(property_summaries) AS property_summaries
        LEFT JOIN (
        SELECT
            name AS property_id,
            service_level,
            google_signals_settings.state AS google_signals_state
        FROM
            analytics_settings_database.ga4_properties
        WHERE
            DATE(_PARTITIONTIME) = CURRENT_DATE()) AS properties
        ON
        property_summaries.property = properties.property_id
        LEFT JOIN (
        SELECT
            property AS property_id,
            type,
            name AS id
        FROM
            analytics_settings_database.ga4_data_streams
        WHERE
            DATE(_PARTITIONTIME) = CURRENT_DATE()) AS data_streams
        ON
        property_summaries.property = data_streams.property_id
        LEFT JOIN (
        SELECT
            property AS property_id,
            name AS id,
            event_name,
            CASE
            WHEN REGEXP_CONTAINS('purchase|first_open|in_app_purchase|app_store_subscription_convert|app_store_subscription_renew', event_name ) THEN 0
            ELSE
            1
        END
            AS is_custom
        FROM
            analytics_settings_database.ga4_conversion_events
        WHERE
            DATE(_PARTITIONTIME) = CURRENT_DATE()) AS conversion_events
        ON
        property_summaries.property = conversion_events.property_id
        LEFT JOIN (
        SELECT
            property AS property_id,
            customer_id
        FROM
            analytics_settings_database.ga4_google_ads_links
        WHERE
            DATE(_PARTITIONTIME) = CURRENT_DATE()) AS google_ads_links
        ON
        property_summaries.property = google_ads_links.property_id
        LEFT JOIN (
        SELECT
            property AS property_id,
            name
        FROM
            analytics_settings_database.ga4_dv360_links
        WHERE
            DATE(_PARTITIONTIME) = CURRENT_DATE()) AS dv360_links
        ON
        property_summaries.property = google_ads_links.property_id
        LEFT JOIN (
        SELECT
            property AS property_id,
            project
        FROM
            analytics_settings_database.ga4_firebase_links
        WHERE
            DATE(_PARTITIONTIME) = CURRENT_DATE()) AS firebase_links
        ON
        property_summaries.property = firebase_links.property_id
        LEFT JOIN (
        SELECT
            property AS property_id,
            parameter_name
        FROM
            analytics_settings_database.ga4_custom_dimensions
        WHERE
            DATE(_PARTITIONTIME) = CURRENT_DATE()) AS custom_dimensions
        ON
        property_summaries.property = custom_dimensions.property_id
        LEFT JOIN (
        SELECT
            property AS property_id,
            parameter_name
        FROM
            analytics_settings_database.ga4_custom_metrics
        WHERE
            DATE(_PARTITIONTIME) = CURRENT_DATE()) AS custom_metrics ON
        property_summaries.property = custom_metrics.property_id
        LEFT JOIN (
        SELECT
            property AS property_id,
            secret_value
        FROM
            analytics_settings_database.ga4_measurement_protocol_secrets
        WHERE
            DATE(_PARTITIONTIME) = CURRENT_DATE()) AS measurement_protocol_secrets
        ON
        property_summaries.property = measurement_protocol_secrets.property_id
        LEFT JOIN (
        SELECT
            property AS property_id,
            name,
            CASE
            WHEN REGEXP_CONTAINS('Purchasers|All Users', display_name) THEN 0
            ELSE
            1
        END as is_custom
        FROM
            analytics_settings_database.ga4_audiences
        WHERE
            DATE(_PARTITIONTIME) = CURRENT_DATE()) AS audiences ON
            property_summaries.property = audiences.property_id
        WHERE
        DATE(_PARTITIONTIME) = CURRENT_DATE()
        GROUP BY
        1, 2, 3, 4, 5, 6
    """
   # table = TableReference(projectId=PROJECT_ID, datasetId=DATASET_ID, tableId=TABLE_ID)
    table_ref = client.dataset(DATASET_ID).table(TABLE_ID)
    job_config = bigquery.QueryJobConfig()
    job_config.write_disposition = 'WRITE_APPEND'
    job_config.destination = table_ref
    query_job = client.query(query, job_config=job_config)
    query_job.result()
    return 'complete'

