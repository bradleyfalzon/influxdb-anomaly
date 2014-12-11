package main

import (
	"fmt"
	"log"
	"time"

	"GoStats/stats"

	"github.com/influxdb/influxdb/client"
)

// Array length which holds the last n points
// to detect breakouts
const breakoutTrackerLen uint = 6

// Number of spikes to occur within the breakoutTracker
// for a breakout to be detected
const breakoutThreshold uint = 4

// Number of points to use for the calculation of the stddev
// this is the "learning" phase, the number of points it will
// look at to calculate a base line and the three sigma.
const movingStdDevLen uint = 60

func main() {
	// create an array of bools that holds n values, this will be used, call it spikes
	spikes := make([]bool, breakoutTrackerLen)

	var tsColName string = "load_1min"
	var tsTableName string = "localhost_load_1min_5.rrd"

	c, err := client.NewClient(&client.ClientConfig{
		Username: "root",
		Password: "root",
		Database: "cacti",
	})
	if err != nil {
		log.Fatal(err)
	}

	query := fmt.Sprintf("select %s from /%s$/ where time > now() - 7d order asc", tsColName, tsTableName)
	//query := fmt.Sprintf("select %s from /%s$/ where time > '2014-12-09 13:30:00' and time < '2014-12-09 15:30' order asc", tsColName, tsTableName)
	results, err := c.Query(query)
	if err != nil {
		log.Fatal(err)
	}

	if len(results) != 1 {
		log.Fatal("Expected 1 time series, recieved %d from query: %s", len(results), query)
	}

	// log.Printf("%#v\n", *results)
	log.Printf("Time series count: %d\n", len(results))
	log.Printf("Name: %s\n", results[0].Name)
	log.Printf("Columns: %v\n", results[0].Columns)

	// Track whether we're alerting or not. If we're alerting
	// we shouldn't adjust the baseline until we're back to normal
	// or until a new breakout has occurred.
	var (
		alerting bool
		thresh   float64
	)

	// Precomputer number of entries we'll be scanning, so we know when we're
	// at the end
	pointsCount := len(results[0].Points)

	movingStdDev := make([]float64, movingStdDevLen)

	for k, v := range results[0].Points {

		if k == pointsCount-1 {
			// We check the current stddev against the next value, so if we're
			// at the end then we can't grab the next value.
			break
		}

		//log.Printf("Spikes: %v\n", spikes)

		tsVal, ok := v[2].(float64)
		if !ok {
			log.Fatalf("Shit mate, sequence number %.f (time %.f, val %#v) aint a float64...", v[1], v[0], v[2])
		}

		// Get the stddev over the previous hour, multiply by 3 to get 3sigma
		// note we might only want to adjust our calculation if the last value
		// was not a spike - as to not cause new spikes to slowly become normal

		movingStdDevPos := k % int(movingStdDevLen)
		movingStdDev[movingStdDevPos] = tsVal

		if k < int(movingStdDevLen) {
			// Mark ourselves as learning until we calculate our stddev
			log.Println("Still learning...")
			continue
		}

		stdDev := stats.StatsSampleStandardDeviation(movingStdDev)
		if !alerting {
			// we're not under normal conditions, so don't think it's normal
			thresh = tsVal + stdDev*4
		}

		err := insertInflux(c, "annotate", results[0].Points[k+1][0], thresh)

		// set k to be the current position to track the spikes
		btPos := k % int(breakoutTrackerLen)

		nextTsVal, ok := results[0].Points[k+1][2].(float64)

		if !ok {
			log.Fatalf("Shit mate, sequence number %.f (time %.f, val %#v) aint no float64...", v[1], v[0], v[2])
		}

		// type assert the time to float64 (cause it is), then convert to int64 and seconds (was ms) for time.Unix
		tsUnix := time.Unix(int64(v[0].(float64)/1000), 0)

		log.Printf("Time: %s, Seq: %.f, Val: %.5f (%v), NVal: %v, Stddev %.5f, 3 sigma: %.5f, threshhold: %.5f",
			tsUnix, v[1], v[2], tsVal, nextTsVal, stdDev, stdDev*3, thresh)

		// Check to ensure we're not above our threshold, it's a greater than
		// to ensure if the line is flat, stddev = 0 so thresh would equal tsVal
		if nextTsVal <= thresh {
			// not an anomaly
			alerting = false
			spikes[btPos] = false
			continue
		}

		// If we're not already alerting, mark this point as a spike
		if !alerting {
			err = annotateInflux(c, "annotate", results[0].Points[k+1][0], "spike")
			if err != nil {
				log.Fatal(err)
			}
		}

		// Mark as alerting, so we don't keep firing off on spikes and we
		// don't adjust our threshold based on alerting conditions
		alerting = true

		// mark this as a spike
		spikes[btPos] = true

		log.Println("Found spike")

		breakout := checkSpikes(&spikes, breakoutThreshold)
		if breakout {
			alerting = false

			err = annotateInflux(c, "annotate", results[0].Points[k+1][0], "breakout")
			if err != nil {
				log.Fatal(err)
			}

			// mark this timestamp as breakout time
			log.Println("Found breakout!!")
		}

	}

	log.Println("Stddev and anomalies have been added to new influxdb table")
}

func checkSpikes(spikes *[]bool, threshold uint) bool {
	var spikeCnt uint = 0
	for _, v := range *spikes {
		if v {
			spikeCnt++
			if spikeCnt > threshold {
				return true
			}
		}
	}
	return false
}

func checkBreakout(spikes *[]bool) bool {
	var spikeCnt uint = 0
	for _, v := range *spikes {
		if v {
			spikeCnt++
			if spikeCnt > breakoutThreshold {
				return true
			}
		}
	}
	return false
}
