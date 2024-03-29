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

echo "******** Welcome **********
*
* Google Analytics Property Overview Table Deployment
*
***************************"

read -p "Please enter your Google Cloud Project ID: " project_id

bq mk -t --time_partitioning_type=DAY \
	$project_id:analytics_settings_database.property_overview

cd query

sql=$(cat property_overview.sql)

bq query \
	--use_legacy_sql=false \
	--destination_table=$project_id:analytics_settings_database.property_overview \
	--display_name="Property Overview" \
	--schedule="every day 23:30" \
	--append_table=true \
	"$sql"

echo "***************************
*
* Google Analytics Property Overview Table Deployment Complete
*
***************************"

