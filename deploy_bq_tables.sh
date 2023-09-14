#!/bin/bash
###########################################################################
#
#  Copyright 2023 Google Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#

cat <<END 
******** Welcome **********
*
* Google Analytics Settings Database Setup
* Create BigQuery Tables
*
***************************
END
read -p "Please enter your Google Cloud Project ID: " project_id

bq mk -d $project_id:analytics_settings_database

echo "~~~~~~~~ Creating BigQuery Tables ~~~~~~~~~~"
cd schemas
bq mk -t --time_partitioning_type=DAY \
	--schema=./ua_account_summaries_schema.json \
	$project_id:analytics_settings_database.ua_account_summaries
bq mk -t --time_partitioning_type=DAY \
	--schema=./ua_goals_schema.json \
	$project_id:analytics_settings_database.ua_goals
bq mk -t --time_partitioning_type=DAY \
	--schema=./ua_views_schema.json \
	$project_id:analytics_settings_database.ua_views
bq mk -t --time_partitioning_type=DAY \
	--schema=./ua_filters_schema.json \
	$project_id:analytics_settings_database.ua_filters
bq mk -t --time_partitioning_type=DAY \
	--schema=./ua_filter_links_schema.json \
	$project_id:analytics_settings_database.ua_filter_links
bq mk -t --time_partitioning_type=DAY \
	--schema=./ua_segments_schema.json \
	$project_id:analytics_settings_database.ua_segments
bq mk -t --time_partitioning_type=DAY \
	--schema=./ua_custom_dimensions_schema.json \
	$project_id:analytics_settings_database.ua_custom_dimensions
bq mk -t --time_partitioning_type=DAY \
	--schema=./ua_custom_metrics_schema.json \
	$project_id:analytics_settings_database.ua_custom_metrics
bq mk -t --time_partitioning_type=DAY \
	--schema=./ua_audiences_schema.json \
	$project_id:analytics_settings_database.ua_audiences
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_account_summaries_schema.json \
	$project_id:analytics_settings_database.ga4_account_summaries
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_accounts_schema.json \
	$project_id:analytics_settings_database.ga4_accounts
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_properties_schema.json \
	$project_id:analytics_settings_database.ga4_properties
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_data_streams_schema.json \
	$project_id:analytics_settings_database.ga4_data_streams
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_measurement_protocol_secrets_schema.json \
	$project_id:analytics_settings_database.ga4_measurement_protocol_secrets
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_conversion_events_schema.json \
	$project_id:analytics_settings_database.ga4_conversion_events
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_custom_dimensions_schema.json \
	$project_id:analytics_settings_database.ga4_custom_dimensions
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_custom_metrics_schema.json \
	$project_id:analytics_settings_database.ga4_custom_metrics
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_firebase_links_schema.json \
	$project_id:analytics_settings_database.ga4_firebase_links
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_google_ads_links_schema.json \
	$project_id:analytics_settings_database.ga4_google_ads_links
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_dv360_link_proposals_schema.json \
	$project_id:analytics_settings_database.ga4_dv360_link_proposals
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_dv360_links_schema.json \
	$project_id:analytics_settings_database.ga4_dv360_links
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_audiences_schema.json \
	$project_id:analytics_settings_database.ga4_audiences
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_sa360_links_schema.json \
	$project_id:analytics_settings_database.ga4_sa360_links
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_bigquery_links_schema.json \
	$project_id:analytics_settings_database.ga4_bigquery_links
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_enhanced_measurement_settings_schema.json \
	$project_id:analytics_settings_database.ga4_enhanced_measurement_settings
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_expanded_data_sets_schema.json \
	$project_id:analytics_settings_database.ga4_expanded_data_sets
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_event_create_rules_schema.json \
	$project_id:analytics_settings_database.ga4_event_create_rules
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_sk_ad_network_conversion_value_schemas_schema.json \
	$project_id:analytics_settings_database.ga4_sk_ad_network_conversion_value_schemas
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_channel_groups_schema.json \
	$project_id:analytics_settings_database.ga4_channel_groups
bq mk -t --time_partitioning_type=DAY \
	--schema=./ga4_adsense_links_schema.json \
	$project_id:analytics_settings_database.ga4_adsense_links
cd ..
echo "BigQuery tables created. BigQuery setup complete. Please return to the README."
