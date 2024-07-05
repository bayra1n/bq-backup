package main

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"

	"github.com/schollz/progressbar/v3"
	"google.golang.org/api/iterator"
)

const (
	defaultRetentionDays = 7
	logFilePath          = "/var/log/bq-backup/backup_log.csv"
	maxLogFileSize       = 10 * 1024 * 1024 // 10MB
	defaultProjectFile   = "project.txt"
)

var webhookURL string
var workspaceWebhookURL string
var tagIDs []string
var messageBuffer []string

func main() {
	projectFile := flag.String("f", defaultProjectFile, "File containing list of project IDs")
	bucketName := flag.String("bucket", "", "GCS bucket name")
	retentionDays := flag.Int("retention", defaultRetentionDays, "Retention period in days")
	webhook := flag.String("webhook", "", "Discord webhook URL")
	workspaceWebhook := flag.String("workspace", "", "Google Workspace Chat webhook URL")
	tagid := flag.String("tagid", "", "Comma-separated list of Discord tag IDs")
	flag.Parse()

	webhookURL = *webhook
	workspaceWebhookURL = *workspaceWebhook
	if *tagid != "" {
		tagIDs = strings.Split(*tagid, ",")
	}

	if *bucketName == "" {
		fmt.Println("Usage: go run main.go -f=PROJECT_FILE --bucket=BUCKET_NAME [--retention=RETENTION_DAYS] [--webhook=WEBHOOK_URL] [--workspace=WORKSPACE_WEBHOOK_URL] [--tagid=TAG_IDS]")
		os.Exit(1)
	}

	projects, err := readProjectFile(*projectFile)
	if err != nil {
		fmt.Printf("Failed to read project file: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		fmt.Printf("Failed to create Storage client: %v\n", err)
		os.Exit(1)
	}
	defer storageClient.Close()

	for _, projectID := range projects {
		client, err := bigquery.NewClient(ctx, projectID)
		if err != nil {
			fmt.Printf("Failed to create BigQuery client for project %s: %v\n", projectID, err)
			continue
		}
		defer client.Close()

		cpuCount := runtime.NumCPU()
		numWorkers := cpuCount / 2

		datasets := listDatasets(ctx, client)
		jobs := make(chan string, len(datasets))
		var wg sync.WaitGroup

		bar := progressbar.NewOptions(len(datasets),
			progressbar.OptionSetDescription(fmt.Sprintf("Backing up datasets for project %s", projectID)),
			progressbar.OptionShowCount(),
			progressbar.OptionSetWidth(30),
			progressbar.OptionSetPredictTime(true),
			progressbar.OptionClearOnFinish(),
			progressbar.OptionSpinnerType(14),
		)

		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for datasetID := range jobs {
					backupDataset(ctx, client, storageClient, *bucketName, projectID, datasetID)
					bar.Add(1)
				}
			}()
		}

		for _, datasetID := range datasets {
			jobs <- datasetID
		}
		close(jobs)

		wg.Wait()

		// Clean up old backups
		cleanupOldBackups(ctx, storageClient, *bucketName, projectID, *retentionDays)
	}
	// Send Google Workspace Chat notification if webhook URL is provided
	if workspaceWebhookURL != "" {
		sendWorkspaceNotification()
	}
}

func readProjectFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var projects []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		project := strings.TrimSpace(scanner.Text())
		if project != "" {
			projects = append(projects, project)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return projects, nil
}

func listDatasets(ctx context.Context, client *bigquery.Client) []string {
	it := client.Datasets(ctx)
	var datasets []string
	for {
		ds, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Printf("Failed to list datasets: %v\n", err)
			os.Exit(1)
		}
		datasets = append(datasets, ds.DatasetID)
	}
	return datasets
}

func backupDataset(ctx context.Context, client *bigquery.Client, storageClient *storage.Client, bucketName, projectID, datasetID string) {
	dataset := client.Dataset(datasetID)
	tables := listTables(ctx, dataset)

	today := time.Now().Format("2006-01-02")
	for _, tableID := range tables {
		table := dataset.Table(tableID)
		meta, err := table.Metadata(ctx)
		if err != nil {
			logStatus(today, projectID, datasetID, tableID, "Failed", fmt.Sprintf("Failed to get metadata: %v", err))
			continue
		}

		if meta.Type == bigquery.ExternalTable {
			// Handle external table export
			tempTableID := fmt.Sprintf("%s_temp_%d", tableID, time.Now().Unix())
			tempTable := dataset.Table(tempTableID)
			if err := createTempTable(ctx, client, tempTable, tableID); err != nil {
				logStatus(today, projectID, datasetID, tableID, "Failed", fmt.Sprintf("Failed to create temporary table: %v", err))
				continue
			}
			if err := backupTable(ctx, tempTable, storageClient, bucketName, projectID, today, datasetID, tableID); err != nil {
				logStatus(today, projectID, datasetID, tableID, "Failed", fmt.Sprintf("Failed to back up table: %v", err))
				_ = tempTable.Delete(ctx)
				continue
			}
			if err := tempTable.Delete(ctx); err != nil {
				fmt.Printf("Failed to delete temporary table %s: %v\n", tempTableID, err)
			}
			logStatus(today, projectID, datasetID, tableID, "Complete", "")
		} else {
			if err := backupTable(ctx, table, storageClient, bucketName, projectID, today, datasetID, tableID); err != nil {
				logStatus(today, projectID, datasetID, tableID, "Failed", fmt.Sprintf("Failed to back up table: %v", err))
				continue
			}
			logStatus(today, projectID, datasetID, tableID, "Complete", "")
		}
	}
}

func listTables(ctx context.Context, dataset *bigquery.Dataset) []string {
	it := dataset.Tables(ctx)
	var tables []string
	for {
		tbl, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Printf("Failed to list tables: %v\n", err)
			os.Exit(1)
		}
		tables = append(tables, tbl.TableID)
	}
	return tables
}

func createTempTable(ctx context.Context, client *bigquery.Client, tempTable *bigquery.Table, sourceTableID string) error {
	query := client.Query(fmt.Sprintf("CREATE TABLE %s AS SELECT * FROM %s", tempTable.FullyQualifiedName(), sourceTableID))
	job, err := query.Run(ctx)
	if err != nil {
		return err
	}
	status, err := job.Wait(ctx)
	if err != nil {
		return err
	}
	return status.Err()
}

func backupTable(ctx context.Context, table *bigquery.Table, storageClient *storage.Client, bucketName, projectID, date, datasetID, tableID string) error {
	basePath := fmt.Sprintf("%s/%s/%s/%s", projectID, date, datasetID, tableID)
	objectPath := fmt.Sprintf("%s/*.avro", basePath)
	gcsURI := fmt.Sprintf("gs://%s/%s", bucketName, objectPath)

	gcsRef := bigquery.NewGCSReference(gcsURI)
	gcsRef.DestinationFormat = bigquery.Avro

	extractor := table.ExtractorTo(gcsRef)
	job, err := extractor.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to start extraction job: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for extraction job: %w", err)
	}

	if err := status.Err(); err != nil {
		return fmt.Errorf("extraction job failed: %w", err)
	}

	return nil
}

func cleanupOldBackups(ctx context.Context, storageClient *storage.Client, bucketName, projectID string, retentionDays int) {
	bucket := storageClient.Bucket(bucketName)
	it := bucket.Objects(ctx, &storage.Query{Prefix: projectID})

	cutoffDate := time.Now().AddDate(0, 0, -retentionDays)
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			fmt.Printf("Failed to list objects for cleanup: %v\n", err)
			break
		}

		if isOlderThanRetention(attrs.Name, cutoffDate) {
			err := bucket.Object(attrs.Name).Delete(ctx)
			if err != nil {
				fmt.Printf("Failed to delete old backup %s: %v\n", attrs.Name, err)
			} else {
				fmt.Printf("Deleted old backup %s\n", attrs.Name)
			}
		}
	}
}

func isOlderThanRetention(objectPath string, cutoffDate time.Time) bool {
	parts := strings.Split(objectPath, "/")
	if len(parts) < 3 {
		return false
	}

	dateStr := parts[1]
	backupDate, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		fmt.Printf("Failed to parse date from path %s: %v\n", objectPath, err)
		return false
	}

	return backupDate.Before(cutoffDate)
}

func logStatus(date, projectID, datasetID, tableID, status, reason string) {
	if err := manageLogFileSize(logFilePath); err != nil {
		fmt.Printf("Failed to manage log file size: %v\n", err)
		return
	}

	file, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Failed to open log file: %v\n", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	logEntry := []string{date, projectID, datasetID, tableID, status, reason}
	if err := writer.Write(logEntry); err != nil {
		fmt.Printf("Failed to write log entry: %v\n", err)
	}

	// Send notification to Discord if webhook URL is provided
	if webhookURL != "" {
		message := fmt.Sprintf("**%s** [`%s`] > %s < - **%s** | Reason: %s", projectID, datasetID, tableID, status, reason)
		sendDiscordNotification(message)
	}

	// Append message to buffer for Google Workspace Chat notification
	messageBuffer = append(messageBuffer, fmt.Sprintf("| *%s* | `%s` | `%s` | `%s` |", projectID, datasetID, status, reason))
}

func manageLogFileSize(filePath string) error {
	// Check the size of the file
	fileInfo, err := os.Stat(filePath)
	if os.IsNotExist(err) {
		return nil // File does not exist, no management needed
	} else if err != nil {
		return err // Return other errors
	}

	// If the file is larger than maxLogFileSize, compress it
	if fileInfo.Size() > maxLogFileSize {
		err := compressLogFile(filePath)
		if err != nil {
			return err
		}

		// Create a new empty log file
		_, err = os.Create(filePath)
		if err != nil {
			return err
		}
	}
	return nil
}

func compressLogFile(filePath string) error {
	zipFilePath := generateZipFileName()
	zipFile, err := os.Create(zipFilePath)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	zipWriter := zip.NewWriter(zipFile)
	defer zipWriter.Close()

	fileToZip, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer fileToZip.Close()

	info, err := fileToZip.Stat()
	if err != nil {
		return err
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}
	header.Name = filepath.Base(filePath)

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, fileToZip)
	if err != nil {
		return err
	}

	return nil
}

func generateZipFileName() string {
	counter := 1
	for {
		zipFileName := fmt.Sprintf("/var/log/bq-backup/backup_log%d.zip", counter)
		if _, err := os.Stat(zipFileName); os.IsNotExist(err) {
			return zipFileName
		}
		counter++
	}
}

func sendWorkspaceNotification() {
	message := "*Backup Daily Big Query " + time.Now().Format("2006-01-02") + "*\n"
	message += "*| `Project` | `Dataset` | `Status` | `Reason` |*\n"
	message += "|-----------------------------------------------------------|\n"
	for _, line := range messageBuffer {
		message += line + "\n"
	}

	workspaceMessage := map[string]string{"text": message}
	workspaceMessageJSON, err := json.Marshal(workspaceMessage)
	if err != nil {
		fmt.Printf("Failed to marshal Google Workspace message: %v\n", err)
		return
	}

	resp, err := http.Post(workspaceWebhookURL, "application/json", bytes.NewBuffer(workspaceMessageJSON))
	if err != nil {
		fmt.Printf("Failed to send Google Workspace notification: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Failed to send Google Workspace notification, received status code: %d\n", resp.StatusCode)
	}
}

func sendDiscordNotification(message string) {
	content := message + "\n\nNote: Project - Dataset - Table - Status - Reason"
	if len(tagIDs) > 0 {
		tags := make([]string, len(tagIDs))
		for i, id := range tagIDs {
			tags[i] = fmt.Sprintf("<@%s>", id)
		}
		tagMessage := strings.Join(tags, " ")
		content = fmt.Sprintf("%s\n\n%s", message, tagMessage)
	}

	embed := map[string]interface{}{
		"title":       "BigQuery Backup Notification",
		"description": content,
		"color":       16711680, // Red color
		"footer": map[string]interface{}{
			"text": "Note : Project - Dataset - Table - Status - Reason",
		},
	}

	discordMessage := map[string]interface{}{
		"content": "",
		"embeds":  []map[string]interface{}{embed},
	}

	discordMessageJSON, err := json.Marshal(discordMessage)
	if err != nil {
		fmt.Printf("Failed to marshal Discord message: %v\n", err)
		return
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(discordMessageJSON))
	if err != nil {
		fmt.Printf("Failed to send Discord notification: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent {
		fmt.Printf("Failed to send Discord notification, received status code: %d\n", resp.StatusCode)
	}
}
