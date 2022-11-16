## Google Analytics Settings Database

This is not an officially supported Google product.

This repository contains code for a Google Cloud Function that loads Universal Google Analytics and Google Analytics 4 settings into a set of BigQuery tables. By default, the function is scheduled to run daily. This creates a daily backup of Google Analytics settings that can be used for a variety of purposes, including restoring settings, auditing setups, and having an extensive change history across accounts.


## Requirements



*   [Google Cloud Platform (GCP) project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) with [billing enabled](https://cloud.google.com/billing/docs/how-to/modify-project#enable-billing) - Create or use an existing project as needed.
    *   Note: This solution uses billable GCP resources.
*   [Google Analytics](https://analytics.google.com/analytics/web/)


## Implementation



1. Navigate to your Google Cloud project and open Cloud Shell
2. Enter the following into Cloud Shell:

```

rm -rf analytics-settings-database && git clone https://github.com/google/analytics-settings-database.git && cd analytics-settings-database && bash deploy.sh

```



3. Enter the information when prompted during the deployment process. When asked if unauthenticated invocations should be allowed for the Cloud Function, answer no.
4. This will create the following:
    *   A Cloud Function (2nd gen)
    *   A Cloud Scheduler Job
    *   A BigQuery dataset with the name “analytics\_settings\_database”
    *   The following tables:
        *   ga4\_account\_summaries
        *   ga4\_accounts
        *   ga4\_audiences
        *   ga4\_android\_app\_data\_streams
        *   ga4\_conversion\_events
        *   ga4\_custom\_dimensions
        *   ga4\_custom\_metrics
        *   ga4\_dv360\_link\_proposals
        *   ga4\_dv360\_links
        *   ga4\_firebase\_links
        *   ga4\_google\_ads\_links
        *   ga4\_ios\_app\_data\_streams
        *   ga4\_measurement\_protocol\_secrets
        *   ga4\_properties
        *   ga4\_web\_data\_streams
        *   ua\_account\_summaries
        *   ua\_audiences
        *   ua\_custom\_dimensions
        *   ua\_custom\_metrics
        *   ua\_filter\_links
        *   ua\_filters
        *   ua\_goals
        *   ua\_segments
        *   ua\_views
5. Add the service account email generated during the deployment process to your Google Analytics accounts.

Upon completing the implementation process, the settings for your Google Analytics accounts that the API can access will be loaded into BigQuery daily at 11 PM. The frequency with which this happens can be adjusted by modifying the Cloud Scheduler Job created during the deployment process.


## (Optional) Deploy Health Checkup Table

This optional table combines several different tables together to quickly see important data related to GA4 properties. The table is set to be updated on a daily basis at 11:30 PM via a scheduled query. The table can be easily connected to a Data Studio dashboard to quickly visualize its data. To create the table and scheduled query, open Cloud Shell and enter the following:

```

cd analytics-settings-database/health\_checkup && bash deploy.sh

```
