package sqs

import (
	"context"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Exporter struct {
	Registry                     *prometheus.Registry
	SQS                          *sqs.SQS
	CloudWatch                   *cloudwatch.CloudWatch
	queueMessagesGauge           *prometheus.GaugeVec
	queueMessagesInFlightGauge   *prometheus.GaugeVec
	queueAgeOfOldestMessageGauge *prometheus.GaugeVec
	queueMessageTotalsGauge      *prometheus.GaugeVec
	exportedTags                 []queueTag
}

type QueueMetric struct {
	QueueName                 string
	Tags                      map[string]string
	Messages                  int64
	MessagesInFlight          int64
	AgeOfOldestMessageSeconds int64
	MessagesTotal             int64
}

type queueTag struct {
	Original   string
	Normalized string
}

func createQueueTag(original string) queueTag {
	return queueTag{
		Original:   original,
		Normalized: normalizeTag(original),
	}
}

func NewExporter(registry *prometheus.Registry, exportedTags []string) *Exporter {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	exportedQueueTags := make([]queueTag, len(exportedTags))
	additionalLabels := make([]string, len(exportedTags))
	for i, tag := range exportedTags {
		queueTag := createQueueTag(tag)
		exportedQueueTags[i] = queueTag
		additionalLabels[i] = queueTag.Normalized
	}

	queueMessageGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sqs_queue_messages",
		Help: "Sqs message queue size.",
	}, append([]string{"queue_name"}, additionalLabels...))

	ageOfOldestMessageGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sqs_queue_oldest_message_age",
		Help: "Sqs queue age of oldest message in seconds.",
	}, append([]string{"queue_name"}, additionalLabels...))

	queueMessagesInFlight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sqs_queue_messages_in_flight",
		Help: "Sqs message in flight messages..",
	}, append([]string{"queue_name"}, additionalLabels...))

	queueMessageTotalsGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sqs_queue_messages_total",
		Help: "Total sqs queue messages both queued and in-flight",
	}, append([]string{"queue_name"}, additionalLabels...))

	registry.MustRegister(queueMessageGauge, ageOfOldestMessageGauge, queueMessagesInFlight, queueMessageTotalsGauge)

	logrus.Info("sqs exported registered")

	return &Exporter{
		Registry:                     registry,
		SQS:                          sqs.New(sess),
		CloudWatch:                   cloudwatch.New(sess),
		queueMessagesGauge:           queueMessageGauge,
		queueAgeOfOldestMessageGauge: ageOfOldestMessageGauge,
		queueMessagesInFlightGauge:   queueMessagesInFlight,
		queueMessageTotalsGauge:      queueMessageTotalsGauge,
		exportedTags:                 exportedQueueTags,
	}
}

func (e *Exporter) Sync() error {
	metrics, err := e.QueuesMetrics()

	if err != nil {
		return err
	}

	for _, metric := range metrics {
		labels := prometheus.Labels{
			"queue_name": metric.QueueName,
		}

		for _, tag := range e.exportedTags {
			labels[tag.Normalized] = metric.Tags[tag.Original]
		}

		e.queueMessagesGauge.
			With(labels).
			Set(float64(metric.Messages))

		e.queueAgeOfOldestMessageGauge.
			With(labels).
			Set(float64(metric.AgeOfOldestMessageSeconds))

		e.queueMessagesInFlightGauge.
			With(labels).
			Set(float64(metric.MessagesInFlight))

		e.queueMessageTotalsGauge.
			With(labels).
			Set(float64(metric.Messages + metric.MessagesInFlight))
	}

	return nil
}

type sqsQueue struct {
	Name string
	Url  string
	Tags map[string]string
}

type queueListRequest struct {
	includeTags bool
}

func (e *Exporter) listQueues(ctx context.Context, req queueListRequest) ([]sqsQueue, error) {
	queueNames, err := e.SQS.ListQueues(&sqs.ListQueuesInput{})

	if err != nil {
		return nil, err
	}

	queues := make([]sqsQueue, len(queueNames.QueueUrls))

	for i, q := range queueNames.QueueUrls {
		var tags = make(map[string]string)

		if req.includeTags {
			queueTags, err := e.SQS.ListQueueTagsWithContext(ctx, &sqs.ListQueueTagsInput{
				QueueUrl: q,
			})

			if err != nil {
				return nil, err
			}

			for name, value := range queueTags.Tags {
				tags[name] = *value
			}
		}

		queues[i] = sqsQueue{
			Name: queueNameFromUrl(*q),
			Url:  *q,
			Tags: tags,
		}
	}
	return queues, nil
}

func (e *Exporter) QueuesMetrics() ([]QueueMetric, error) {
	queues, err := e.listQueues(context.TODO(), queueListRequest{
		// skip tags api request if no exported tags are requested
		includeTags: len(e.exportedTags) > 0,
	})

	if err != nil {
		return nil, err
	}

	logrus.Infof("fetching metrics for %d queues", len(queues))

	results := make(chan *QueueMetric, len(queues))
	errors := make(chan error, len(queues))

	begin := time.Now()

	var wg sync.WaitGroup
	wg.Add(len(queues))
	for _, queue := range queues {
		go func(queue sqsQueue) {
			defer wg.Done()
			info, err := e.fetchQueuesMetrics(queue)
			if err != nil {
				errors <- err
			}
			results <- info
		}(queue)
	}

	wg.Wait()

	close(results)
	close(errors)

	resultsSlice := make([]QueueMetric, 0)
	for r := range results {
		resultsSlice = append(resultsSlice, *r)
	}

	// TODO we should merge errors here
	for e := range errors {
		return nil, e
	}

	logrus.Infof("metrics scraping completed for %d queues, duration: %d ms", len(resultsSlice), time.Since(begin).Milliseconds())
	return resultsSlice, nil
}

func (e *Exporter) QueueAgeOfOldestMessage(queueName string) (int64, error) {
	metrics, err := e.CloudWatch.GetMetricStatistics(&cloudwatch.GetMetricStatisticsInput{
		Statistics: []*string{aws.String("Maximum")},
		Namespace:  aws.String("AWS/SQS"),
		Dimensions: []*cloudwatch.Dimension{{
			Name:  aws.String("QueueName"),
			Value: aws.String(queueName),
		}},
		MetricName: aws.String("ApproximateAgeOfOldestMessage"),
		StartTime:  aws.Time(time.Now().Add(-10 * time.Minute)),
		EndTime:    aws.Time(time.Now()),
		Period:     aws.Int64(10 * 60), // 10 minutes to have 1 datapoints
	})

	if err != nil {
		return 0, err
	}

	age := int64(0)
	if metrics.Datapoints != nil {
		age = int64(*metrics.Datapoints[0].Maximum)
	} else {
		logrus.Infof("no ApproximateAgeOfOldestMessage metrics found for queue %s", queueName)
	}

	return age, nil
}

func (e *Exporter) fetchQueuesMetrics(queue sqsQueue) (*QueueMetric, error) {
	req, err := e.SQS.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: aws.StringSlice([]string{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"}),
		QueueUrl:       &queue.Url,
	})

	if err != nil {
		return nil, err
	}

	messages, err := strconv.ParseInt(*req.Attributes["ApproximateNumberOfMessages"], 10, 64)

	if err != nil {
		return nil, err
	}

	messagesInFlight, err := strconv.ParseInt(*req.Attributes["ApproximateNumberOfMessagesNotVisible"], 10, 64)

	if err != nil {
		return nil, err
	}

	ageOfOldestMessage := int64(0)

	if messages > 0 {
		ageOfOldestMessage, err = e.QueueAgeOfOldestMessage(queue.Name)
		if err != nil {
			return nil, err
		}
	}

	return &QueueMetric{
		QueueName:                 queue.Name,
		Tags:                      queue.Tags,
		Messages:                  messages,
		AgeOfOldestMessageSeconds: ageOfOldestMessage,
		MessagesInFlight:          messagesInFlight,
	}, nil
}

func queueNameFromUrl(url string) string {
	return url[strings.LastIndex(url, "/")+1:]
}

var wordsRe = regexp.MustCompile(`[^A-Za-z\d]+`)

func normalizeTag(name string) string {
	name = strings.ToLower(name)
	name = wordsRe.ReplaceAllString(name, "_")
	return name
}
