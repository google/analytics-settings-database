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
* Google Analytics Settings Database
* Property Overview
*
***************************
END
read -p "Please enter your Google Cloud Project ID: " project_id

exit_setup () {
  exit "Exiting. Setup failed."
}

create_cloud_function () {
  cd function
  gcloud functions deploy $function_name \
  	--project=$project_id \
  	--runtime=python311 \
  	--memory=2GB \
  	--timeout=3600s \
  	--trigger-http \
  	--entry-point=main \
		--gen2
}

read -p "Please enter your desired function name. The recommended
function name is 'property-overview': " function_name

echo "~~~~~~~~ Creating Function ~~~~~~~~~~"
	
if create_cloud_function; then
  echo "Function created."
else
  read -p  "Function creation failed. Try again? y/n: " exit_response
  if [ $exit_response = "n" ]; then
    exit_setup
  else
    cloud_function_setup
  fi
fi

echo "Cloud function created."

echo "~~~~~~~~ Creating BigQuery Table ~~~~~~~~~~"

bq mk -t --time_partitioning_type=DAY \
	$project_id:analytics_settings_database.property_overview

cd ../workflow

read -p "Please enter your settings downloader function URL: " downloader_url

read -p "Please enter your property overview function URL: " property_overview_url

read -p "Please enter your settings downloader service account email: " downloader_service_account

sed -i -e "s|{downloader-function-url}|$downloader_url|g" property_overview.yaml

sed -i -e "s|{property-overview-function-url}|$property_overview_url|g" property_overview.yaml

echo "~~~~~~~~ Creating Workflow ~~~~~~~~~~"
echo "**** Enable Workflows API ****"
gcloud services enable workflows.googleapis.com

gcloud workflows deploy property_overview \
  --source=property_overview.yaml \ 
  --service-account=$downloader_service_account
  
echo "***************************
  *
  * Google Analytics Settings Database
  * Property Overview workflow created. Return to readme.
  *
  ***************************"
