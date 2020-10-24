package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/unsafe"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	prometheusCommon "github.com/webdevops/go-prometheus-common"
	"net/http"
	"time"
)

func probeKustoQueryHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	var timeoutSeconds float64
	var endpoint, database, query, mappingValueColumn string
	params := r.URL.Query()

	startTime := time.Now()

	contextLogger := buildContextLoggerFromRequest(r)

	// If a timeout is configured via the Prometheus header, add it to the request.
	timeoutSeconds, err = getPrometheusTimeout(r, ProbeKustoScrapeTimeoutDefault)
	if err != nil {
		contextLogger.Error(err)
		http.Error(w, fmt.Sprintf("failed to parse timeout from Prometheus header: %s", err), http.StatusInternalServerError)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSeconds*float64(time.Second)))
	defer cancel()
	r = r.WithContext(ctx)

	if endpoint, err = paramsGetRequired(params, "endpoint"); err != nil {
		contextLogger.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if database, err = paramsGetRequired(params, "database"); err != nil {
		contextLogger.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if query, err = paramsGetRequired(params, "query"); err != nil {
		contextLogger.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	mappingValueColumn = paramsGetWithDefault(params, "mappingValueColumn", "")

	result, err := azureKustoMetrics.Query(ctx, endpoint, database, kusto.NewStmt("", kusto.UnsafeStmt(unsafe.Stmt{Add: true})).UnsafeAdd(query))

	if err != nil {
		contextLogger.Error(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
	}

	registry := prometheus.NewRegistry()

	queryInfoGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "azurerm_kusto_query_result",
		Help: "Azure Kusto Data Explorer query result",
	}, []string{})
	registry.MustRegister(queryInfoGauge)

	queryInfoGauge.With(prometheus.Labels{}).Set(boolToFloat64(result.Result != nil))

	metricsList := prometheusCommon.NewMetricsList()
	metricsList.SetCache(metricsCache)

	if result.Result != nil {
		var metricLabels []string

		if len(*result.Result) > 1 && result.Result != nil {
			for _, columnName := range (*result.Result)[0].ColumnNames {
				if columnName != mappingValueColumn {
					metricLabels = append(metricLabels, columnName)
				}
			}
		}

		kustoRowGauge := prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "azurerm_kusto_query_row",
			Help: "Azure Kusto Data Explorer query row",
		}, metricLabels)
		registry.MustRegister(kustoRowGauge)

		for _, row := range *result.Result {
			rowLabels := prometheus.Labels{}

			for i := range row.ColumnNames {
				columnName := row.ColumnNames[i]
				valueStr := row.Values[i]

				if mappingValueColumn != columnName {
					rowLabels[columnName] = valueStr
				}
			}

			metricsList.AddInfo(rowLabels)
		}

		metricsList.GaugeSet(kustoRowGauge)
	}

	// global stats counter
	prometheusCollectTime.With(prometheus.Labels{
		"subscriptionID": "",
		"handler":        ProbeLoganalyticsScrapeUrl,
		"filter":         query,
	}).Observe(time.Now().Sub(startTime).Seconds())

	h := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	h.ServeHTTP(w, r)
}
