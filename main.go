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
	for k, v := range m {
		// TODO check for invalid characters in k, v
		key, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		val, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		list = append(list, fmt.Sprintf("%s=%s", key, val))
	}
	return list, nil
}

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
