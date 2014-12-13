package detector

import "github.com/influxdb/influxdb/client"

func insertInflux(c *client.Client, tableName string, time interface{}, key string, val interface{}) (err error) {

	points := make([]interface{}, 2)

	points[0] = time
	points[1] = val

	series := &client.Series{
		Name:    tableName,
		Columns: []string{"time", key},
		Points:  [][]interface{}{points},
	}
	if err := c.WriteSeries([]*client.Series{series}); err != nil {
		return err
	}

	return

}
