package main

import (
	"context"
	"github.com/Azure/azure-kusto-go/kusto"
	"github.com/Azure/azure-kusto-go/kusto/data/table"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"os"
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
	Result *[]KustoRow
}

func (m *AzureKustoMetrics) QueryClient(endpoint string) *kusto.Client {
	if m.client == nil {
		m.clientMutex.Lock()
		//auth, err := auth.NewAuthorizerFromEnvironment()
		//authorizer := kusto.Authorization{Authorizer: auth}

		kustoTenantId, _ := os.LookupEnv("AZURE_KUSTO_TENANT_ID")
		kustoClientId, _ := os.LookupEnv("AZURE_KUSTO_CLIENT_ID")
		kustoClientSecret, _ := os.LookupEnv("AZURE_KUSTO_CLIENT_SECRET")

		authorizer := kusto.Authorization{
			Config: auth.NewClientCredentialsConfig(kustoClientId, kustoClientSecret, kustoTenantId),
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

type KustoRow struct {
	ColumnNames []string
	Values      []string
}

func (m *AzureKustoMetrics) Query(ctx context.Context, endpoint string, database string, query kusto.Stmt) (*AzureKustoMetricsResult, error) {
	ret := AzureKustoMetricsResult{}

	// Query our database table "systemNodes" for the CollectionTimes and the NodeIds.
	iter, err := m.QueryClient(endpoint).Query(ctx, database, query)
	if err != nil {
		panic(err)
	}
	defer iter.Stop()

	// .Do() will call the function for every row in the table.
	var recs []KustoRow
	err = iter.Do(
		func(row *table.Row) error {
			rec := KustoRow{}
			for _, columnName := range row.ColumnTypes {
				rec.ColumnNames = append(rec.ColumnNames, columnName.Name)
			}
			for _, val := range row.Values {
				rec.Values = append(rec.Values, val.String())
			}
			recs = append(recs, rec)
			return nil
		},
	)
	ret.Result = &recs

	return &ret, err
}
