#!/bin/bash
###########################################################################
#
#  Copyright 2021 Google Inc.
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

echo "******** Welcome **********
*
* Google Analytics Settings Database Setup
*
***************************"
echo "---------------------------"
read -p "Please enter your Google Cloud Project ID: " project_id
echo "---------------------------"
echo "~~~~~~~~ Enabling APIs ~~~~~~~~~~"
gcloud services enable \
cloudbuild.googleapis.com \
cloudfunctions.googleapis.com \
storage-component.googleapis.com \
storage-api.googleapis.com \
bigquery.googleapis.com \
cloudscheduler.googleapis.com \
appengine.googleapis.com \
analytics.googleapis.com \
analyticsadmin.googleapis.com \
--async

create_cloud_bucket () {
	read -p "Please enter your Cloud Bucket name: " cloud_bucket_name
	echo "~~~~~~~~ Creating Cloud Bucket ~~~~~~~~~~"
	{
		gsutil mb gs://$cloud_bucket_name
		echo "-----------------------
		
Bucket creation complete.
		
-----------------------"
	} || {
		echo "-----------------------
		
Bucket creation failed. Enter a different name.
		
-----------------------"
		create_cloud_bucket
	}
}

create_cloud_bucket

create_service_account () {
	read -p "Please enter you desired service account name with no spaces.
This service account will be used by your Cloud Function.
The recommended name is 'ga-database' : " service_account_name
	echo "~~~~~~~~ Creating Service Account ~~~~~~~~~~"
	{
		gcloud iam service-accounts create $service_account_name --display-name=$service_account_name
		service_account_email=$(gcloud iam service-accounts list --filter=displayName=$service_account_name --format='value(email)')
		gcloud projects add-iam-policy-binding $project_id \
			--member='serviceAccount:${service_account_email}' \
			--role='roles/editor'
		echo "--------------------------------
		
Service account creation complete.
		
--------------------------------"
	} || {
		echo "--------------------------------
		
Service account creation failed.
		
--------------------------------"
		create_service_account
	}
}

create_service_account

create_cloud_function () {
	read -p "Please enter your desired Function name. The recommended
function name is 'analytics_settings_downloader': " function_name
	{
		cd settings_downloader_function
		echo "~~~~~~~~ Creating Function ~~~~~~~~~~"
		gcloud functions deploy $function_name \
			--project=$project_id \
			--runtime=python39 \
			--service-account=$service_account_email \
			--memory=1GB \
			--timeout=540s \
			--trigger-http \
			--entry-point=ga_settings_download \
			--set-env-vars=BUCKET_NAME=$cloud_bucket_name
		cd ..
		echo "-------------------------
		
Function creation complete.
		
-------------------------"
	} || {
		echo "-------------------------
		
Function creation failed.
		
-------------------------"
		create_cloud_function
	}
}

create_cloud_function

echo "~~~~~~~~ Creating BigQuery Dataset ~~~~~~~~~~"
bq mk -d $project_id:analytics_settings_database
echo "~~~~~~~~ Creating BigQuery Tables ~~~~~~~~~~"
cd schemas
bq mk -t --time_partitioning_type=DAY \
	--schema=/ua_account_summaries_schemas.json \
	$project_id:_schema.ua_account_summaries
bq mk -t --time_partitioning_type=DAY \
	--schema=/ua_goals_schema.json \
	$project_id:analytics_settings_database.ua_goals
bq mk -t --time_partitioning_type=DAY \
	--schema=/ua_views_schema.json \
	$project_id:analytics_settings_database.ua_views
bq mk -t --time_partitioning_type=DAY \
	--schema=/ua_filters_schema.json \
	$project_id:analytics_settings_database.ua_filters
bq mk -t --time_partitioning_type=DAY \
	--schema=/ua_filters_schema.json \
	$project_id:analytics_settings_database.ua_filter_links
bq mk -t --time_partitioning_type=DAY \
	--schema=/ua_segments_schema.json \
	$project_id:analytics_settings_database.ua_segments
bq mk -t --time_partitioning_type=DAY \
	--schema=/ua_custom_dimensions_schema.json \
	$project_id:analytics_settings_database.ua_custom_dimensions
bq mk -t --time_partitioning_type=DAY \
	--schema=/ua_custom_metrics_schema.json \
	$project_id:analytics_settings_database.ua_custom_metrics
bq mk -t --time_partitioning_type=DAY \
	--schema=/ua_audiences_schema.json \
	$project_id:analytics_settings_database.ua_audiences
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_account_summaries_schema.json \
	$project_id:analytics_settings_database.ga4_account_summaries
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_accounts_schema.json \
	$project_id:analytics_settings_database.ga4_accounts
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_properties_schema.json \
	$project_id:analytics_settings_database.ga4_properties
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_android_app_data_streams_schema.json \
	$project_id:analytics_settings_database.ga4_android_app_data_streams
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_measurment_protocol_secrets_schema.json \
	$project_id:analytics_settings_database.ga4_measurement_protocol_secrets
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_conversion_events_schema.json \
	$project_id:analytics_settings_database.ga4_conversion_events
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_custom_dimensions_schema.json \
	$project_id:analytics_settings_database.ga4_custom_dimensions
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_custom_metrics_schema.json \
	$project_id:analytics_settings_database.ga4_custom_metrics
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_firebase_links_schema.json \
	$project_id:analytics_settings_database.ga4_firebase_links
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_google_ads_links_schema.json \
	$project_id:analytics_settings_database.ga4_google_ads_links
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_ios_app_data_streams_schema.json \
	$project_id:analytics_settings_database.ga4_ios_app_data_streams
bq mk -t --time_partitioning_type=DAY \
	--schema=/ga4_web_data_streams_schema.json \
	$project_id:analytics_settings_database.ga4_web_data_streams
cd ..
echo "-------------------------

All tables created.

-------------------------"

create_scheduler () {
	read -p "Please enter your desired Cloud Scheduler name.
The recommended scheduler name is 'analytics_settings_downloader': " scheduler_name
	echo "A cloud scheduler will now be created that runs daily at 11 PM."
	{
		echo "~~~~~~~~ Creating Cloud Scheduler ~~~~~~~~~~"
		function_uri=$(gcloud functions describe $function_name --format='value(httpsTrigger.url)')
		gcloud scheduler jobs create http $scheduler_name
			--schedule "0 23 * * *" \
			--uri=$function_uri \
			--http-method=GET \
		  --oidc-service-account-email=$service_account_email \
			--oidc-token-audience=$function_uri \
		  --project=$project_name
			echo "-------------------------
		
Cloud scheduler creation complete.
		
-------------------------"
	} || {
		echo "-------------------------
		
Cloud Scheduler failed. Please create the scheduler manually.
		
-------------------------"
	}
}

create_scheduler

echo "***************************
*
* Google Analytics Settings Database Setup Complete!
*
* You must now grant ${service_account_email} access to your Google Analytics
* Accounts. This will be the email Google Cloud uses to access your Google
* Analytics settings.
***************************"
