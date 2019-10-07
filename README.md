# sqs-exporter
[![Go Report Card](https://goreportcard.com/badge/github.com/GaruGaru/aws-sqs-exporter)](https://goreportcard.com/report/github.com/GaruGaru/aws-sqs-exporter)
[![Travis Card](https://travis-ci.org/GaruGaru/aws-sqs-exporter.svg?branch=master)](https://travis-ci.org/GaruGaru/aws-sqs-exporter)
[![MicroBadger Size](https://img.shields.io/microbadger/image-size/garugaru/sqs-exporter)](https://cloud.docker.com/u/garugaru/repository/docker/garugaru/sqs-exporter)
 
Prometheus exporter for aws sqs service with async refresh and cloudwatch integration 

## Running

### Docker
```bash
docker run -it \
 -e AWS_REGION=<region> \
 -e AWS_ACCESS_KEY_ID=<access-key> \
 -e AWS_SECRET_ACCESS_KEY=<secret> \
 -p 9999:9999 \
 garugaru/sqs-exporter
```

### Binary
```bash
sqs-exporter -refresh 60 # Refresh every 60s
```

## Metrics

Every supported metrics is labelled with *queue_name* 

* sqs_queue_messages
* sqs_queue_messages_in_flight
* sqs_queue_oldest_message_age