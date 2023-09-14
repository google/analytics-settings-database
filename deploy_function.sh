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
* Create Cloud Function
*
***************************
END
read -p "Please enter your Google Cloud Project ID: " project_id

exit_setup () {
  exit "Exiting Google Analytics Settings Database setup. Setup failed."
}

create_cloud_function () {
  gcloud functions deploy $function_name \
  	--project=$project_id \
  	--runtime=python311 \
  	--memory=2GB \
  	--timeout=3600s \
  	--trigger-http \
  	--entry-point=ga_settings_download \
		--gen2
}

read -p "Please enter your desired Function name. The recommended
function name is 'analytics-settings-downloader': " function_name

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

echo "Cloud function created. Please return to the README."
