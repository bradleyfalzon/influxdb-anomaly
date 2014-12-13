package main

import (
	"log"

	"github.com/bradleyfalzon/influxdb-anomaly/detector"
	"github.com/influxdb/influxdb/client"
)

const (
	breakoutTrackerLen uint = 6
	breakoutThreshold  uint = 4
	movingStdDevLen    uint = 60
)

func main() {

	var tsColName string = "load_1min"
	var tsTableName string = "localhost_load_1min_5.rrd"

	d := new(detector.Detector)

	d.Debug = true

	influxClient := &client.ClientConfig{
		Username: "root",
		Password: "root",
		Database: "cacti",
	}

	err := d.SetInfluxDB(influxClient)
	if err != nil {
		log.Fatal(err)
	}

	influxQuery := &detector.InfluxQuery{
		Table:   tsTableName,
		Col:     tsColName,
		DaysAgo: 1,
	}

	err = d.LoadInfluxDB(influxQuery)
	if err != nil {
		log.Fatal(err)
	}

	sigmaConfig := &detector.SigmaConfig{
		BreakoutTrackerLen: breakoutTrackerLen,
		BreakoutThreshold:  breakoutThreshold,
		MovingStdDevLen:    movingStdDevLen,
		Sigmas:             3,
	}

	err = d.ProcessSigma(sigmaConfig)
	if err != nil {
		log.Fatal(err)
	}

	resultsConfig := &detector.ResultsConfig{
		SaveThresholds: true,
		Table:          tsTableName + "_anomaly",
	}

	err = d.SaveResults(resultsConfig)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Stddev and anomalies have been added to new influxdb table")
}
