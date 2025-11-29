from dagster import job, op
import clickhouse_connect

@op
def test_clickhouse_connection(context):
    try:
        client = clickhouse_connect.get_client(
            host="localhost",
            port=8123
        )

        result = client.query("SELECT version()")
        context.log.info(f"CONNECTED! ClickHouse version: {result.result_rows[0][0]}")

    except Exception as e:
        context.log.error(f"FAILED: {e}")
        raise e


@job
def test_ch_job():
    test_clickhouse_connection()
