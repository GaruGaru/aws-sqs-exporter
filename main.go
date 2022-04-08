package main

import (
	"flag"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"net/http"
	"sqs-exporter/sqs"
	"strings"
	"time"
)

func main() {
	addr := flag.String("addr", "0.0.0.0", "web server bind address")
	port := flag.Int("port", 9999, "web server port")
	metricsPath := flag.String("path", "/metrics", "exporter metrics path")
	refreshRate := flag.Int("refresh", 1*60, "refresh delay in seconds")
	exportedTags := flag.String("exported-tags", "", "comma separated list of tags exported as labels (eg. owner,scope)")
	flag.Parse()

	log.SetFormatter(&log.TextFormatter{FullTimestamp: true})
	registry := prometheus.NewRegistry()

	var exportedTagsList []string
	if len(*exportedTags) != 0 {
		exportedTagsList = strings.Split(*exportedTags, ",")
	}

	sqsExporter := sqs.NewExporter(registry, exportedTagsList)

	ticker := time.NewTicker(time.Duration(*refreshRate) * time.Second)
	done := make(chan bool)

	err := sqsExporter.Sync()

	if err != nil {
		panic(err)
	}

	go func() {
		for {
			select {
			case <-done:
				ticker.Stop()
				return
			case <-ticker.C:
				if err := sqsExporter.Sync(); err != nil {
					log.Error(err)
				}
			}
		}
	}()

	http.Handle(*metricsPath, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))

	bind := fmt.Sprintf("%s:%d", *addr, *port)

	log.Infof("started metrics server on %s", bind)

	if err := http.ListenAndServe(bind, nil); err != nil {
		done <- true
		panic(err)
	}
}
