package billingexportnotification

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"

	"cloud.google.com/go/compute/metadata"

	"github.com/GoogleCloudPlatform/functions-framework-go/functions"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"

	"github.com/slack-go/slack"
)

// global variables
var (
	projectId          string
	billingAccountId   string
	datasetId          string
	partitionTableName string
	slackOauthToken    string
	slackChannelId     string
	detectAbnormalyPercentageStr string
)

// about type: https://pkg.go.dev/cloud.google.com/go/bigquery#InferSchema
type BillingTableRows struct {
	Proj          string               `bigquery:"proj"`
	ServiceName   string               `bigquery:"service_name"`
	YesterdayCost bigquery.NullFloat64 `bigquery:"yesterday_cost"`
	TodayCost     bigquery.NullFloat64 `bigquery:"today_cost"`
	ChangeRate    bigquery.NullFloat64 `bigquery:"change_rate"`
}

func init() {
	functions.HTTP("BillingNotiFunc", billingNotiFunc)
}

// if using cloud function,
// "serviceName": "bigquery.googleapis.com",
// "methodName": "google.cloud.bigquery.v2.JobService.InsertJob",
func billingNotiFunc(w http.ResponseWriter, r *http.Request) {

	var err error
	projectId = os.Getenv("PROJECT_ID")
	if projectId == "" {
		fmt.Println("PROJECT_ID environment variable is not set, instead using metadata.")

		c := metadata.NewClient(&http.Client{})
		projectId, err = c.ProjectID()
		if err != nil {
			fmt.Println("failed get metadata.ProjectID: %v", err)
			os.Exit(1)
		}
	}

	billingAccountId = os.Getenv("BILLING_ACCOUNT_ID")
	if billingAccountId == "" {
		fmt.Println("BILLING_ACCOUNT_ID environment variable must be set.")
		os.Exit(1)
	}

	datasetId = os.Getenv("DATASET_ID")
	if datasetId == "" {
		fmt.Println("DATASET_ID environment variable must be set.")
		os.Exit(1)
	}

	partitionTableName = os.Getenv("PARTITION_TABLE_NAME")
	if partitionTableName == "" {
		fmt.Println("PARTITION_TABLE_NAME environment variable must be set.")
		os.Exit(1)
	}

	slackOauthToken = os.Getenv("SLACK_OAUTH_TOKEN")
	if slackOauthToken == "" {
		fmt.Println("SLACK_OAUTH_TOKEN environment variable must be set.")
		os.Exit(1)
	}

	slackChannelId = os.Getenv("SLACK_CHANNEL_ID")
	if slackChannelId == "" {
		fmt.Println("SLACK_CHANNEL_ID environment variable must be set.")
		os.Exit(1)
	}

	detectAbnormalyPercentageStr = os.Getenv("DETECT_ABNORMALY_PERCENTAGE")
	if detectAbnormalyPercentageStr == "" {
		fmt.Println("DETECT_ABNORMALY_PERCENTAGE environment variable must be set.")
		os.Exit(1)
	}
	detectAbnormalyPercentage, err := strconv.ParseFloat(detectAbnormalyPercentageStr, 64)
	if err != nil  {
		fmt.Println("failed parse string to float64. check DETECT_ABNORMALY_PERCENTAGE environment variable.")
		os.Exit(1)
	}

	ctx := context.Background()

	client, err := bigquery.NewClient(ctx, projectId)
	if err != nil {
		log.Fatalf("bigquery.NewClient: %v", err)
	}
	defer client.Close()

	_, err = queryPartitionTable(ctx, client)
	if err != nil {
		log.Fatal(err)
	}

	rowsIter, err := queryChangeRate(ctx, client)
	if err != nil {
		log.Fatal(err)
	}

	rows, err := convertToList(rowsIter)
	if err != nil {
		log.Fatal(err)
	}

	printResults(os.Stdout, &rows)

	if err := detectAbnormalyCostService(&rows, detectAbnormalyPercentage); err != nil {
		log.Fatal(err)
	}

}

// query returns a row iterator suitable for reading query results.
func query(ctx context.Context, client *bigquery.Client, sql string) (*bigquery.RowIterator, error) {
	query := client.Query(sql)
	return query.Read(ctx)
}

// create or replace partition table for query.
func queryPartitionTable(ctx context.Context, client *bigquery.Client) (*bigquery.RowIterator, error) {
	return query(ctx, client,
		`CREATE OR REPLACE TABLE FUNCTION `+fmt.Sprintf("`%s.%s.%s`", projectId, datasetId, partitionTableName)+`(part_date STRING)
		AS (
			SELECT project.id AS proj, service.description AS service_name, sum(cost) AS cost
			FROM `+fmt.Sprintf("`%s.%s.gcp_billing_export_v1_%s`", projectId, datasetId, billingAccountId)+`
			WHERE CAST(DATE(_PARTITIONTIME) AS STRING) = part_date
			GROUP BY project.id, service.description
		);`)
}

// get chage rate from partition tables.
func queryChangeRate(ctx context.Context, client *bigquery.Client) (*bigquery.RowIterator, error) {
	table := fmt.Sprintf("`%s.%s`", datasetId, partitionTableName)
	return query(ctx, client,
		`SELECT 
		today.proj, 
		today.service_name, 
		yesterday.cost AS yesterday_cost, 
		today.cost AS today_cost,
		ROUND(SAFE_MULTIPLY(SAFE_DIVIDE((today.cost - yesterday.cost), yesterday.cost),100), 2) AS change_rate,
		FROM `+table+`(SAFE_CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) AS STRING)) AS today, `+
			table+`(SAFE_CAST(DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY) AS STRING)) AS yesterday
		WHERE today.proj=yesterday.proj
		AND today.service_name = yesterday.service_name`)
}

func convertToList(iter *bigquery.RowIterator) (rows []BillingTableRows, err error) {
	for {
		var row BillingTableRows
		err := iter.Next(&row)
		if err == iterator.Done {
			break
		}
		if err != nil {
			return rows, fmt.Errorf("error iterating through results: %w", err)
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func printResults(w io.Writer, rows *[]BillingTableRows) {
	for _, row := range *rows {
		// if row.ChangeRate.Valid {
		// } else {
		// }

		projStr := fmt.Sprintf("project: %s", row.Proj)
		serviceNameStr := fmt.Sprintf("service: %s", row.ServiceName)
		yesterdayCostStr := fmt.Sprintf("yesterday cost: %.2f", row.YesterdayCost.Float64)
		todayCostStr := fmt.Sprintf("today cost: %.2f", row.TodayCost.Float64)
		changeRateStr := fmt.Sprintf("change rate: %.2f", row.ChangeRate.Float64)

		fmt.Fprintf(w, "%-40s | %-45s | %-25s | %-25s | %-10s\n", projStr, serviceNameStr, yesterdayCostStr, todayCostStr, changeRateStr)
		// fmt.Fprintf(w, "project: %s service: %s yesterday cost: %.2f today cost: %.2f change rate: %.2f\n", row.Proj, row.ServiceName, row.YesterdayCost.Float64, row.TodayCost.Float64, row.ChangeRate.Float64)
	}
}

func detectAbnormalyCostService(rows *[]BillingTableRows, abnormalyPercentage float64) error {
	for _, row := range *rows {
		if row.ChangeRate.Valid {
			if row.ChangeRate.Float64 > abnormalyPercentage {
				projStr := fmt.Sprintf("project: %s", row.Proj)
				serviceNameStr := fmt.Sprintf("service: %s", row.ServiceName)
				yesterdayCostStr := fmt.Sprintf("yesterday cost: %.2f", row.YesterdayCost.Float64)
				todayCostStr := fmt.Sprintf("today cost: %.2f", row.TodayCost.Float64)
				changeRateStr := fmt.Sprintf("change rate: %.2f", row.ChangeRate.Float64)
				payload := fmt.Sprintf("abnormaly service detection -> %-40s | %-45s | %-25s | %-25s | %-10s\n", projStr, serviceNameStr, yesterdayCostStr, todayCostStr, changeRateStr)
				err := sendToSlackChannel(slackChannelId, slackOauthToken, &payload)
				if err != nil {
					return fmt.Errorf("%s %v", "slack.api.PostMessage", err)
				}
			}
		}
	}
	return nil
}

func sendToSlackChannel(channelId string, oauthToken string, payload *string) (err error) {
	if channelId == "" {
		return fmt.Errorf("slack channel id is null")
	}
	if oauthToken == "" {
		return fmt.Errorf("slack oauth token is null")
	}

	api := slack.New(oauthToken)
	// https://api.slack.com/reference/surfaces/formatting#escaping
	_, _, err = api.PostMessage(
		channelId,
		slack.MsgOptionText(*payload, false),
	)
	if err != nil {
		return fmt.Errorf("%s %v", "slack.api.PostMessage", err)
	}
	return nil
}
