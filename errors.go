package influx

type ErrorReporter struct {
	Database
	tags   map[string]interface{}
	values map[string]interface{}
}

func (e ErrorReporter) Error(name string) (err error) {
	return e.Write(name, e.tags, e.values)
}
