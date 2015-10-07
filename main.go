package influx

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"
)

const INFLUX_MAX_BUF = 1024
const INFLUX_TIMEOUT = 5 * time.Second

type InfluxDataPoint string

type Database struct {
	http     http.Client
	c        chan InfluxDataPoint
	shutdown chan struct{}
	done     chan struct{}
	writeURL string
}

func (d Database) NewMetric(name string, hostname string, tags map[string]interface{}, values map[string]interface{}) *Metric {
	if tags == nil {
		tags = make(map[string]interface{})
	}
	tags["hostname"] = hostname
	tags["type"] = "metric"
	return &Metric{d, name, tags, values}
}

func (d Database) ErrorReporter(hostname string, tags map[string]interface{}, values map[string]interface{}) *ErrorReporter {
	if tags == nil {
		tags = make(map[string]interface{})
	}
	if values == nil {
		values = make(map[string]interface{})
	}
	tags["hostname"] = hostname
	tags["type"] = "error"
	values["value"] = 1
	return &ErrorReporter{d, tags, values}
}

func (d Database) Write(name string, tags map[string]interface{}, values map[string]interface{}) (err error) {
	tags_list, err := join_kv(tags)
	if err != nil {
		return err
	}
	values_list, err := join_kv(values)
	if err != nil {
		return err
	}
	names := append([]string{name}, tags_list...)

	select {
	case d.c <- InfluxDataPoint(fmt.Sprintf("%s %s %d", strings.Join(names, ","), strings.Join(values_list, ","), time.Now().UnixNano())):
	default:
		return fmt.Errorf("Rejecting data point due to full buffer")
	}
	return nil
}

func join_kv(m map[string]interface{}) (s []string, err error) {
	var list = make([]string, 0, len(m))
	for key, v := range m {
		// TODO check for invalid characters in k, v
		val, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		list = append(list, fmt.Sprintf("%s=%s", key, val))
	}
	return list, nil
}

/*
func encode_thing(thing interface{}) (s string, err error) {
	v := reflect.ValueOf(thing)
	switch v.Kind() {
	case reflect.String:
		s = v.String()
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s = fmt.Sprintf("%d", v.Uint())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s = fmt.Sprintf("%d", v.Int())
	case reflect.Float32, reflect.Float64:
		s = fmt.Sprintf("%f", v.Float())
	default:
		return "", fmt.Errorf("Unknown kind %s", v.Kind())
	}
	return s, nil
}
*/

func (d Database) run() {
	var shutdown bool
	tick := time.NewTicker(1 * time.Second)
	var data []string
	for {
		if shutdown {
			d.done <- struct{}{}
			return
		}
		select {
		case _ = <-d.shutdown:
			close(d.c)
			for x := range d.c {
				data = append(data, string(x))
			}
			shutdown = true
		case x := <-d.c:
			data = append(data, string(x))
			if len(data) < INFLUX_MAX_BUF {
				continue
			}
		case _ = <-tick.C:
		}
		if len(data) == 0 {
			continue
		}
		if response, err := d.http.Post(d.writeURL, "text/line-protocol", bytes.NewBufferString(strings.Join(data, "\n"))); err != nil {
			log.Printf("Error submitting to influxdb: %s\n", err)
		} else if response.StatusCode != 204 {
			log.Printf("Unexpected return code %d: %s submitting to influxdb\n", response.StatusCode, response.Status)
		} else {
			data = data[0:0]
		}
	}
}

func (d Database) Finalize() {
	d.shutdown <- struct{}{}
	_ = <-d.done
}

func New(host string, database string) (*Database, error) {
	d := Database{
		c:        make(chan InfluxDataPoint, INFLUX_MAX_BUF),
		writeURL: fmt.Sprintf("http://%s:8086/write?db=%s", host, database),
		http:     http.Client{Timeout: INFLUX_TIMEOUT},
		done:     make(chan struct{}),
		shutdown: make(chan struct{}),
	}
	go d.run()
	return &d, nil
}
