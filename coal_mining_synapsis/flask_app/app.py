from flask import Flask, render_template
from etl import run_etl
import clickhouse_connect
import pandas as pd

app = Flask(__name__)

@app.route('/run_etl')
def etl_endpoint():
    try:
        print("Running ETL...")
        df, corr, p, forecast_value = run_etl()
        print("ETL finished.")
        return render_template(
            'dashboard.html',
            table=df.to_dict(orient='records'),
                               corr=corr,
                               p=p,
                               forecast=forecast_value)
    except Exception as e:
        print(f"ETL error: {e}")
        return f"Error running ETL: {e}", 500


@app.route('/')
def dashboard():
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, user='default', password='clickhousepwd')
    df = client.query_df("SELECT * FROM daily_production_metrics ORDER BY date")
    corr, p = None, None
    return render_template('dashboard.html', table=df.to_dict(orient='records'), corr=corr, p=p)

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)
