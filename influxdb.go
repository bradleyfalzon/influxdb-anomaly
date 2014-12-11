package main

import "github.com/influxdb/influxdb/client"

func annotateInflux(c *client.Client, tableName string, time interface{}, title string) (err error) {

	points := make([]interface{}, 2)

	points[0] = time
	points[1] = title

	series := &client.Series{
		Name:    tableName,
		Columns: []string{"time", "title"},
		Points:  [][]interface{}{points},
	}
	if err := c.WriteSeries([]*client.Series{series}); err != nil {
		return err
	}

	return

}

func insertInflux(c *client.Client, tableName string, time interface{}, val float64) (err error) {

	points := make([]interface{}, 2)

	points[0] = time
	points[1] = val

	series := &client.Series{
		Name:    tableName,
		Columns: []string{"time", "thresh"},
		Points:  [][]interface{}{points},
	}
	if err := c.WriteSeries([]*client.Series{series}); err != nil {
		return err
	}

	return

}
