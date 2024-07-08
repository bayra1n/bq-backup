# BigQuery Backup Script

This script automates the backup of BigQuery datasets to Google Cloud Storage (GCS) and sends notifications to Discord and Google Workspace using webhooks.

## Features

* **Multi-Project Support:** Back up datasets from multiple Google Cloud projects listed in a text file.
* **GCS Storage:** Stores backups in your specified GCS bucket.
* **Retention Policy:**  Configurable retention period to automatically delete old backups.
* **Discord Notifications:** Sends success/failure notifications to a Discord channel via webhook.
* **Tagging:**  Allows you to mention specific users or roles in Discord notifications.
* **Google Workspace Notifications:**  (Optional) Sends notifications to Google Workspace channels.

## Installation

1. **Install the Google Cloud SDK:** Follow the instructions at [Install the gcloud CLI](https://cloud.google.com/sdk/docs/install) to set up the Google Cloud command-line tools.
2. **Permissions:** Ensure the account running the script has the necessary permissions in Google Cloud to access BigQuery and GCS.
3. **Download bq-backup:**
   ```
   curl -L -o bq-backup https://github.com/bayra1n/bq-backup/releases/download/latest/bq-backup
   ```

## Configuration

1. **Create Backup Directory:**

   ```bash
   sudo mkdir -p /var/log/bq-backup/
   sudo chown $USER /var/log/bq-backup/
   ```
   
2. **Prepare Project File (`projects.txt`):**

    * Create a text file named `projects.txt` in the same directory as the script.
    * List each Google Cloud project ID on a separate line.

    ```
    your-project-id-1
    your-project-id-2
    your-project-id-3
    ```
3. **Export ENV File:**
    ```bash
    export DISCORD="[YOUR_DISCORD_WEBHOOK_URL]"
    export TAG="[USERID_1],[ROLEID_1],..."
    export GWS="[YOUR_GOOGLE_WORKSPACE_WEBHOOK_URL]"
    export GCS="[YOUR_GCS_BUCKET_NAME]"

    ```

## Full Usage

```bash
sudo ./bq-backup -f projects.txt --bucket=$GCS --retention=30 --webhook=$DISCORD --tagid=$TAG --workspace=$GWS
```

## Usage

```bash
sudo ./bq-backup --bucket=$GCS --retention=30
```

* **`-f`:** Path to the project file (defaults to `projects.txt`).
* **`--bucket`:** Name of your GCS bucket.
* **`--retention`:** Number of days to retain backups (default is 7).
* **`--webhook`:** Discord webhook URL.
* **`--tagid`:** Comma-separated list of Discord tag IDs (e.g., `4123124123123,545435436111`).
* **`--workspace`:** Google Workspace webhook URL (optional).

**How it works:**

- This example will back up all datasets from these 3 projects to your GCS bucket, retain backups for 30 days, and send notifications to your specified Discord channel and Google Workspace webhook URL.

## Contributing

Contributions are welcome! Feel free to open issues or submit pull requests.

## License

This script is licensed under the [MIT License](LICENSE).
