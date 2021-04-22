package sqs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/cloudwatch"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
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
}

type QueueMetric struct {
	QueueName                 string
	Messages                  int64
	MessagesInFlight          int64
	AgeOfOldestMessageSeconds int64
	MessagesTotal             int64
}

func NewExporter(registry *prometheus.Registry) *Exporter {
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		SharedConfigState: session.SharedConfigEnable,
	}))

	queueMessageGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sqs_queue_messages",
		Help: "Sqs message queue size.",
	}, []string{"queue_name"})

	ageOfOldestMessageGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sqs_queue_oldest_message_age",
		Help: "Sqs queue age of oldest message in seconds.",
	}, []string{"queue_name"})

	queueMessagesInFlight := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sqs_queue_messages_in_flight",
		Help: "Sqs message in flight messages..",
	}, []string{"queue_name"})

	queueMessageTotalsGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "sqs_queue_messages_total",
		Help: "Total sqs queue messages both queued and in-flight",
	}, []string{"queue_name"})

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

func (e *Exporter) QueuesMetrics() ([]QueueMetric, error) {

	queues, err := e.SQS.ListQueues(&sqs.ListQueuesInput{})

	if err != nil {
		return nil, err
	}

	logrus.Infof("fetching metrics for %d queues", len(queues.QueueUrls))

	results := make(chan *QueueMetric, len(queues.QueueUrls))
	errors := make(chan error, len(queues.QueueUrls))

	begin := time.Now()

	var wg sync.WaitGroup
	wg.Add(len(queues.QueueUrls))
	for _, queueUrl := range queues.QueueUrls {
		go func(url *string) {
			defer wg.Done()
			info, err := e.fetchQueuesMetrics(url)
			if err != nil {
				errors <- err
			}
			results <- info
		}(queueUrl)
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

	logrus.Infof("metrics scraping completed for %d queues, duration: %d ms", len(resultsSlice), time.Now().Sub(begin).Milliseconds())
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

func (e *Exporter) fetchQueuesMetrics(url *string) (*QueueMetric, error) {
	req, err := e.SQS.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		AttributeNames: aws.StringSlice([]string{"ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"}),
		QueueUrl:       url,
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

	queueName := queueNameFromUrl(*url)

	ageOfOldestMessage := int64(0)

	if messages > 0 {
		ageOfOldestMessage, err = e.QueueAgeOfOldestMessage(queueName)
		if err != nil {
			return nil, err
		}
	}

	return &QueueMetric{
		QueueName:                 queueName,
		Messages:                  messages,
		AgeOfOldestMessageSeconds: ageOfOldestMessage,
		MessagesInFlight:          messagesInFlight,
	}, nil
}

func queueNameFromUrl(url string) string {
	return url[strings.LastIndex(url, "/")+1:]
}
