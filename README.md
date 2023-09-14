## Google Analytics Settings Database

This is not an officially supported Google product.

This repository contains code for a Google Cloud Function that loads Google Analytics 4 settings into a set of BigQuery tables. By default, the function is scheduled to run daily. This creates a daily backup of Google Analytics settings that can be used for a variety of purposes, including restoring settings, auditing setups, and having an extensive change history across accounts.

### Requirements

- [Google Cloud Platform (GCP) project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) with [billing enabled](https://cloud.google.com/billing/docs/how-to/modify-project#enable-billing) - Create or use an existing project as needed.
    - Note: This solution uses billable GCP resources.
- [Google Analytics](https://analytics.google.com/analytics/web/)

### Implementation

1. Navigate to your Google Cloud and enable the [Google Analytics Admin API](https://console.cloud.google.com/apis/library/analyticsadmin.googleapis.com)
2. Navigate to [IAM & Admin > Service Accounts ](https://console.cloud.google.com/iam-admin/serviceaccounts) and create a new service account.
    - Give the service account a name. This guide will use analytics-settings-database as an example.
    - Grant the service account the BigQuery Admin role.
    - Grant the service account the Cloud Funtions Invoker role.
3. Click on your newly created service account and navigate to "Keys".
4. Create a new JSON key. Make note of the file that was downloaded to your device.
5. Open cloud shell and enter the following to create the cloud function:

    ```

    rm -rf analytics-settings-database && git clone https://github.com/google/analytics-settings-database.git && cd analytics-settings-database && bash deploy_function.sh

    ```
    - Follow the steps outlined in the deploy script to create the HTTP function.
    - Once the function has been created, navigate to [Cloud Functions](https://console.cloud.google.com/functions/list) and click on the cloud function.
    - Edit the function and under "Runtime, build, connections and security settings", change the service account to the one you recently created.
    - Click next.
    - Click "+" to create a new file. Name this file credentials.json and add the contents of the key file you downloaded earlier afer you created your service account.
    - Click deploy. Your cloud function should now be operational.
6. If it is not already open, open cloud shell again and enter the following to create your BigQuery dataset and tables. This will automatically create a data set named "analytics\_settings\_database" and populate it with the required tables.
    ```

    cd analytics-settings-database && bash deploy_bq_tables.sh

    ```
    - Once the script has run, your new tables should be visible in BigQuery.
7. Navigate to [Cloud Scheduler](https://console.cloud.google.com/cloudscheduler).
    - Create a job.
    - Give the schedule a name. This guide will use analytics-settings-database as an example.
    - Enter a frequency. You can enter whatever frequency you would like, but this guide will use 0 22 * * * as an example to run the scheduler daily at 10 PM (or 22:00).
    - Enter your timezone.
    - Set the target type to HTTP.
    - Set the URL to your cloud function's URL.
    - Set the Auth header to "Add OIDC token" and select your service account.
    - Save the schedule.
8. Copy your service account email and grant it access to the GA4 accounts you want it to access. 

Upon completing the implementation process, the settings for your Google Analytics accounts that the API can access will be loaded into BigQuery daily at 10 PM. The frequency with which this happens can be adjusted by modifying the Cloud Scheduler Job created during the deployment process.
