package detector

import (
	"GoStats/stats"
	"errors"
	"fmt"
	"log"
	"time"

	influxClient "github.com/influxdb/influxdb/client"
)

type Detector struct {
	Debug         bool
	influxClient  *influxClient.Client
	InfluxResults []*influxClient.Series
	Results       []*ResultRow
}

type InfluxQuery struct {
	Table   string
	Col     string
	DaysAgo uint
}

type ResultRow struct {
	Time interface{}
	Cols []ResultCol
}

type ResultCol struct {
	K string
	V interface{}
}

type ResultsConfig struct {
	SaveThresholds bool
	Table          string
}

type SigmaConfig struct {
	// Array length which holds the last n points
	// to detect breakouts.
	BreakoutTrackerLen,

	// Number of spikes to occur within the breakoutTracker
	// for a breakout to be detected.
	BreakoutThreshold,

	// Number of points to use for the calculation of the stddev
	// this is the "learning" phase, the number of points it will
	// look at to calculate a base line and the three sigma.
	MovingStdDevLen,

	// Number of sigmas to set as the threshold, usually 3.
	Sigmas uint
}

// Set the client options for influx db connections
func (d *Detector) SetInfluxDB(c *influxClient.ClientConfig) (err error) {
	d.influxClient, err = influxClient.NewClient(c)
	return
}

func (d *Detector) LoadInfluxDB(q *InfluxQuery) (err error) {

	query := fmt.Sprintf("select %s from /%s$/ where time > now() - %dd order asc", q.Col, q.Table, q.DaysAgo)

	if d.Debug {
		log.Println("LoadInfluxDB Query:", query)
	}

	d.InfluxResults, err = d.influxClient.Query(query)
	if err != nil {
		return
	}

	if len(d.InfluxResults) != 1 {
		return errors.New(fmt.Sprintf("Expected 1 time series, recieved %d from query: %s", len(d.InfluxResults), query))
	}

	if d.Debug {
		log.Printf("Time series count: %d\n", len(d.InfluxResults))
		log.Printf("Name: %s\n", d.InfluxResults[0].Name)
		log.Printf("Columns: %v\n", d.InfluxResults[0].Columns)
	}

	return

}

func (d *Detector) ProcessSigma(c *SigmaConfig) (err error) {

	// Track whether we're alerting or not. If we're alerting
	// we shouldn't adjust the baseline until we're back to normal
	// or until a new breakout has occurred.
	var (
		alerting bool
		thresh   float64
	)

	// create an array of bools that holds n values, this will be used, call it spikes
	spikes := make([]bool, c.BreakoutTrackerLen)

	// Precomputer number of entries we'll be scanning, so we know when we're
	// at the end
	pointsCount := len(d.InfluxResults[0].GetPoints())

	// Track the previous standard deviations
	movingStdDev := make([]float64, c.MovingStdDevLen)

	cols := d.InfluxResults[0].GetColumns()
	const (
		COL_TIME = iota
		COL_SEQ
		COL_VAL
	)

	for k, v := range d.InfluxResults[0].GetPoints() {

		if k == pointsCount-1 {
			// We check the current stddev against the next value, so if we're
			// at the end then we can't grab the next value.
			break
		}

		// Keep track of the results
		result := ResultRow{Time: v[COL_TIME]}

		tsVal, ok := v[COL_VAL].(float64)
		if !ok {
			return errors.New(fmt.Sprintf("Sequence number %.f (time %.f, val %#v) aint a float64...", v[COL_SEQ], v[COL_TIME], v[COL_VAL]))
		}

		// Get the stddev over the previous hour, multiply by 3 to get 3sigma
		// note we might only want to adjust our calculation if the last value
		// was not a spike - as to not cause new spikes to slowly become normal

		movingStdDevPos := k % int(c.MovingStdDevLen)
		movingStdDev[movingStdDevPos] = tsVal

		if k < int(c.MovingStdDevLen) {
			// Mark ourselves as learning until we calculate our stddev
			if d.Debug {
				log.Println("Still learning...")
			}
			continue
		}

		stdDev := stats.StatsSampleStandardDeviation(movingStdDev)
		if !alerting {
			// we're not under normal conditions, so don't think it's normal
			thresh = tsVal + stdDev*float64(c.Sigmas)
		}

		// Grab the thresholds and save it to memory
		result.Cols = append(result.Cols, ResultCol{K: cols[COL_VAL] + "_upper", V: thresh})

		btPos := k % int(c.BreakoutTrackerLen)

		nextTsVal, ok := d.InfluxResults[0].Points[k+1][COL_VAL].(float64)

		if !ok {
			log.Fatalf("Sequence number %.f (time %.f, val %#v) aint no float64...", v[COL_SEQ], v[COL_TIME], v[COL_VAL])
		}

		// type assert the time to float64 (cause it is), then convert to int64 and seconds (was ms) for time.Unix
		tsUnix := time.Unix(int64(v[COL_TIME].(float64)/1000), 0)

		if d.Debug {
			log.Printf("Time: %s, Seq: %.f, Col: %s, Val: %.5f (%v), NVal: %v, Stddev %.5f, sigma: %.5f, threshhold: %.5f",
				tsUnix, v[COL_SEQ], cols[COL_VAL], v[COL_VAL], tsVal, nextTsVal, stdDev, stdDev*float64(c.Sigmas), thresh)
		}

		// Check to ensure we're not above our threshold, it's a greater than
		// to ensure if the line is flat, stddev = 0 so thresh would equal tsVal
		if nextTsVal <= thresh {
			// not an anomaly
			alerting = false
			spikes[btPos] = false
			goto endloop
		}

		if d.Debug {
			log.Println("Found spike")
		}

		// If we're not already alerting, mark this point as a spike
		if !alerting {
			// Save the results to memory
			result.Cols = append(result.Cols, ResultCol{K: cols[COL_VAL] + "_annotate", V: "spike"})
		}

		// Mark as alerting, so we don't keep firing off on spikes and we
		// don't adjust our threshold based on alerting conditions
		alerting = true

		// mark this as a spike
		spikes[btPos] = true

		if checkSpikes(&spikes, c.BreakoutThreshold) {
			alerting = false

			result.Cols = append(result.Cols, ResultCol{K: cols[COL_VAL] + "_annotate", V: "breakout"})

			// mark this timestamp as breakout time
			if d.Debug {
				log.Println("Found breakout!!")
			}
		}

	endloop:
		d.Results = append(d.Results, &result)
	}

	return
}

func (d *Detector) SaveResults(c *ResultsConfig) (err error) {

	for _, row := range d.Results {
		for _, cols := range row.Cols {
			if d.Debug {
				log.Printf("Inserting into table: %v, time: %v, k: %v, v: %v\n",
					c.Table, row.Time, cols.K, cols.V)
			}
			err = insertInflux(d.influxClient, c.Table, row.Time, cols.K, cols.V)
			if err != nil {
				return
			}
		}
	}

	return
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
