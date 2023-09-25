# Spreaker connector for BigQuery

Upload Spreaker podcast statistics to Google BigQuery. Uses JSON credentials (obviously not uploaded to GitHub) to access the service
account permissions for GCP, and to access Spreaker data. The usage is:

## How to use

1. Clone the repository locally
2. Run `python -r requirements.txt` to install needed libraries (specially if you use a different _venv_)
3. Add the *credentials.json* and *auth.json* files to the folder (or in another, but providing the path to the file)
4. Run `python -m main.py -show ID -table TABLE` to run the upload process, replacing ID and TABLE with the corresponding values. That's it!

The code is automated as a Google Cloud Function for automated retrieval of data for each day, although it can be run manually
in order to force the upload of data in case an error happens.

## Description

The uploaded data will have the following structure (where the last column is the one that allows us to plot **evolutions of the metrics**):

| COLUMN | TYPE | DESCRIPTION |
|:---:|:---:|:---:|
| episode_title | STRING | Title to identify the podcast |
| plays_count | INTEGER |  |
| plays_ondemand_count | INTEGER |  |
| plays_live_count | INTEGER |  |
| chapters_count | INTEGER |  |
| messages_count | INTEGER |  |
| likes_count | INTEGER |  |
| downloads_count | INTEGER |  |
| published_at | DATE | Date of publishing in Spreaker |
| date | DATE | Date when the script was executed |

The last column essentially logs when that data was taken, and will serve as the datetime for the plots. An example:

    I run the script today, and we have 10 podcasts up. We will get 10 rows in the BQ table, each one with the current metrics for today.

    Now I run it next week, and in the middle we have uploaded a new podcast. Now, the script ADDS 11 new rows (no overwriting) for each podcast,
    with their values, and the date column as the present day.

This ensures that we keep a timeline evolution, not only to add new podcasts, but to see if **previous ones had changes in their metrics** (which
they obviously will).

The file contains CLI arguments, so you can modify the following parameters:

- **show**: the Spreaker ID for the show. You can check yours with the following API request (it's an open API so no need for auth):

        https://api.spreaker.com/v2/search?type=episodes&q=CLIENT_NAME

    which will show all podcasts where the title contains the matched string.

- **date**: when to start the upload. The logic returns all the podcasts for the specified show **AFTER** that date. Defaults to
`2020-01-01`, but any date before the first podcast publishing is fine.

- **credentials**: path to the JSON file for the GCP project. As a way to ensure no mistakes can be made, the `.gitignore` file
**avoids uploading any JSON data**.

- **auth**: path to the JSON file for Spreaker. Also not uploaded due to the `.gitignore` file. Needed to obtain the statistics through the API.

- **project + dataset + table**: self explanatory; you can also select where to upload the data in BQ. For this, the BQ credentials must allow
access first.

The *auth.json* file expects the following structure:

    {
        "TOKEN": 'XXXXX'
    }

replacing *XXXXX* with the provided Spreaker OAuth key.
