package influx

import "time"

type Metric struct {
	Database
	name   string
	tags   map[string]interface{}
	values map[string]interface{}
}

func (m Metric) WriteTime(startTime time.Time) (err error) {
	values := make(map[string]interface{})
	for k, v := range m.values {
		values[k] = v
	}
	values["value"] = time.Since(startTime).Seconds()
	err = m.Write(m.name, m.tags, values)
	return err
}
