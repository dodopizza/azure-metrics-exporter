package main

import (
	"context"
	"fmt"
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"sync"
)

type AzureKustoMetrics struct {
	client      *kusto.Client
	clientMutex sync.Mutex
}

func NewAzureKustoMetrics() *AzureKustoMetrics {
	ret := AzureKustoMetrics{}
	return &ret
}

type AzureKustoMetricsResult struct {
	Result *[]table.Row
}

func (m *AzureKustoMetrics) QueryClient(endpoint string) *kusto.Client {
	if m.client == nil {
		m.clientMutex.Lock()
		authorizer := kusto.Authorization{
			Config: auth.NewClientCredentialsConfig("clientId", "clientSecret", "tenantId"),
		}

		client, err := kusto.New(endpoint, authorizer)
		if err != nil {
			panic("add error handling")
		}
		m.client = client
		m.clientMutex.Unlock()
	}

	return m.client
}

func (m *AzureKustoMetrics) Query(ctx context.Context, endpoint string, database string, query kusto.Stmt) (*AzureKustoMetricsResult, error) {
	ret := AzureKustoMetricsResult{}

	// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
	iter, err := m.QueryClient(endpoint).Query(ctx, database, query)
	if err != nil {
		panic("add error handling")
	}
	defer iter.Stop()

	// .Do() will call the function for every row in the table.
	var recs []table.Row
	err = iter.Do(
		func(row *table.Row) error {
			//recs = append(recs, *row)
			fmt.Println(row)
			return nil
		},
	)
	if err != nil {
		panic("add error handling")
	}
	ret.Result = &recs

	return &ret, err
}
