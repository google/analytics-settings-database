#!/bin/bash
###########################################################################
#
#  Copyright 2022 Google Inc.
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
*
***************************
END
read -p "Please enter your Google Cloud Project ID: " project_id
echo "~~~~~~~~ Enabling APIs ~~~~~~~~~~"
gcloud services enable \
cloudbuild.googleapis.com \
cloudfunctions.googleapis.com \
storage-component.googleapis.com \
bigquery.googleapis.com \
cloudscheduler.googleapis.com \
appengine.googleapis.com \
analytics.googleapis.com \
analyticsadmin.googleapis.com \
--async

gcloud app create

exit_setup () {
  exit "Exiting Google Analytics Settings Database setup. Setup failed."
}

create_service_account () {
  gcloud iam service-accounts create $service_account_name \
    --display-name=$service_account_name
}

set_service_account_email () {
  if [[ service_account_email = "" || $1 < 3 ]]; then
    echo "Service account email attempt $1"
    sleep 2
    retry_attempt=$(( $1 + 1 ))
    set_service_account_email "$retry_attempt"
  else
    echo $service_account_email
  fi
}

set_service_account_iam_policy () {
  gcloud projects add-iam-policy-binding $project_id \
    --member="serviceAccount:$service_account_email" \
    --role="roles/editor"
}

service_account_setup () {
  read -p "Please enter you desired service account name with no spaces.
This service account will be used by your Cloud Function.
The recommended name is 'ga-database' : " service_account_name
  echo "~~~~~~~~ Creating Service Account ~~~~~~~~~~"
  if create_service_account; then
    service_account_email="$service_account_name@$project_id.iam.gserviceaccount.com"
    if set_service_account_iam_policy; then
      echo "Service account created."
    else
      read -p  "Service account creation failed. Try again? y/n: " exit_response
      if [ $exit_response = "n" ]; then
        exit_setup
      else
        service_account_setup
      fi
    fi
  else
    read -p  "Service account creation failed. Try again? y/n: " exit_response
    if [ $exit_response = "n" ]; then
      exit_setup
    else
      create_service_account
    fi
  fi
}

service_account_setup

create_cloud_function () {
  gcloud functions deploy $function_name \
  	--project=$project_id \
  	--runtime=python310 \
  	--service-account=$service_account_email \
  	--memory=1GB \
  	--timeout=3600s \
  	--trigger-http \
  	--entry-point=ga_settings_download \
		--gen2
}

cloud_function_setup () {
	read -p "Please enter your desired Function name. The recommended
function name is 'analytics_settings_downloader': " function_name
  cd settings_downloader_function
  echo "~~~~~~~~ Creating Function ~~~~~~~~~~"
	if create_cloud_function; then
	  cd ..
	  echo "Function created."
  else
    cd ..
    read -p  "Function creation failed. Try again? y/n: " exit_response
    if [ $exit_response = "n" ]; then
      exit_setup
    else
      cloud_function_setup
    fi
  fi
}

cloud_function_setup

echo "~~~~~~~~ Creating BigQuery Dataset ~~~~~~~~~~"
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
cd ..
echo "BigQuery tables created."

create_cloud_scheduler () {
  gcloud scheduler jobs create http $scheduler_name \
  	--schedule "0 23 * * *" \
    --uri="$function_uri" \
  	--http-method=GET \
  	--oidc-service-account-email=$service_account_email \
    --oidc-token-audience=$function_uri \
    --project=$project_id
}

cloud_scheduler_setup () {
	read -p "Please enter your desired Cloud Scheduler name.
The recommended scheduler name is 'analytics_settings_downloader': " scheduler_name
  echo "A cloud scheduler will now be created that runs daily at 11 PM."
	echo "~~~~~~~~ Creating Cloud Scheduler ~~~~~~~~~~"
	function_uri=$(gcloud functions describe $function_name --format="value(httpsTrigger.url)")
	echo $function_uri
  if gcloud app browse; then
    gcloud app create
  fi
	if create_cloud_scheduler; then
    echo "Cloud scheduler created."
  else
    cd ..
    read -p  "Schedule job creation failed. Try again? y/n: " exit_response
    if [ $exit_response = "n" ]; then
      exit_setup
    else
      cloud_scheduler_setup
    fi
  fi
}

cloud_scheduler_setup

echo "***************************
*
* Google Analytics Settings Database Setup Complete!
*
* You must now grant $service_account_email access to your Google Analytics
* Accounts. This will be the email Google Cloud uses to access your Google
* Analytics settings.
***************************"