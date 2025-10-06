package frac

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_model/go"
)

type NopCounter struct {
}

func (n NopCounter) Desc() *prometheus.Desc {
	return nil
}

func (n NopCounter) Write(metric *io_prometheus_client.Metric) error {
	return nil
}

func (n NopCounter) Describe(descs chan<- *prometheus.Desc) {

}

func (n NopCounter) Collect(metrics chan<- prometheus.Metric) {

}

func (n NopCounter) Inc() {

}

func (n NopCounter) Add(f float64) {

}
