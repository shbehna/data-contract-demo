# Data Contracts using... DataContract 

This example illustrate the [Datacontract](https://datacontract.com) language to define a data contract for the NYC yellow cab trip data. 

# Set environment variables

Set the following environment variables to point to your Databricks Workspace. 

| Environment Variable                        | Example                                   | Description                                               |
|----------------------------------------------|-------------------------------------------|-----------------------------------------------------------|
| `DATACONTRACT_DATABRICKS_TOKEN`              | dapia00000000000000000000000000000        | The personal access token to authenticate                 |
| `DATACONTRACT_DATABRICKS_HTTP_PATH`          | /sql/1.0/warehouses/b053a3ffffffff        | The HTTP path to the SQL warehouse or compute cluster     |
| `DATACONTRACT_DATABRICKS_SERVER_HOSTNAME`    | dbc-abcdefgh-1234.cloud.databricks.com    | The host name of the SQL warehouse or compute cluster     |

# Run the example

```sh
cd datacontract
pip install -r requirements.txt
datacontract test nyc_yellow_taxi_trip.datacontract.yaml
```