import pandas as pd
import requests
import pymysql
import clickhouse_connect
import logging
import time
import warnings
from datetime import datetime
from scipy.stats import pearsonr
from statsmodels.tsa.arima.model import ARIMA

# --- Logging ---
logging.basicConfig(
    filename="etl_errors.log",
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s:%(message)s"
)

# --- Fetch Data ---
def fetch_production_logs(mysql_conn):
    return pd.read_sql("SELECT date, mine_id, shift, tons_extracted, quality_grade FROM production_logs", mysql_conn)

def fetch_sensors_data(path='data/equipment_sensors.csv'):
    df = pd.read_csv(path, parse_dates=['timestamp'])
    print("Sensor data loaded")
    return df

def fetch_weather_for_date(date_str):
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude=2.0167&longitude=117.3000"
        f"&start_date={date_str}&end_date={date_str}"
        f"&daily=temperature_2m_mean,precipitation_sum"
        f"&timezone=Asia/Jakarta"
    )
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        daily = data.get('daily', {})
        t_list = daily.get('temperature_2m_mean')
        p_list = daily.get('precipitation_sum')

        if isinstance(t_list, list) and len(t_list) > 0 and isinstance(p_list, list) and len(p_list) > 0:
            return {'date': date_str, 'temp_mean': t_list[0], 'precipitation': p_list[0]}
        else:
            raise ValueError("Weather data not available or empty.")
    except Exception as e:
        logging.warning(f"Weather fetch failed for {date_str}: {e}")
        return {'date': date_str, 'temp_mean': 0.0, 'precipitation': 0.0}

# --- Transform ---
def compute_metrics(production_df, sensor_df):
    production_df['tons_extracted'] = pd.to_numeric(production_df['tons_extracted'], errors='coerce').fillna(0)
    production_df['quality_grade'] = pd.to_numeric(production_df['quality_grade'], errors='coerce')
    production_df['tons_extracted'] = production_df['tons_extracted'].apply(lambda x: max(x, 0))
    production_df = production_df.dropna(subset=['quality_grade'])

    daily = production_df.groupby('date').agg(
        total_production_daily=('tons_extracted', 'sum'),
        average_quality_grade=('quality_grade', 'mean')
    ).reset_index()

    sensor_df['date'] = sensor_df['timestamp'].dt.date
    ops = sensor_df.groupby(['date', 'equipment_id']).apply(
        lambda g: pd.Series({
            'active_hours': (g['status'] == 'active').sum(),
            'total_hours': len(g),
            'fuel': g['fuel_consumption'].sum(skipna=True)
        }),
        include_groups=False
    ).reset_index()

    eq = ops.groupby('date').agg(
        total_active_hours=('active_hours', 'sum'),
        total_hours=('total_hours', 'sum'),
        total_fuel=('fuel', 'sum')
    ).reset_index()

    eq['equipment_utilization'] = (eq['total_active_hours'] / eq['total_hours'] * 100).clip(0, 100)

    merged = pd.merge(daily, eq, on='date', how='left')
    merged['fuel_efficiency'] = merged.apply(
        lambda r: r['total_fuel'] / r['total_production_daily'] if r['total_production_daily'] > 0 else None,
        axis=1
    )

    return merged

def enrich_with_weather(df):
    records = []
    for i, d in enumerate(df['date']):
        date_str = pd.to_datetime(d).strftime("%Y-%m-%d")
        print(f"Fetching weather for {date_str} ({i+1}/{len(df)})")
        rec = fetch_weather_for_date(date_str)
        records.append(rec)
        time.sleep(0.3)
    wdf = pd.DataFrame(records)
    wdf['date'] = pd.to_datetime(wdf['date']).dt.date
    return pd.merge(df, wdf, on='date', how='left')

def compute_weather_impact(df):
    try:
        rainy = df[df['precipitation'] > 0]['total_production_daily']
        non_rainy = df[df['precipitation'] == 0]['total_production_daily']
        if len(rainy) > 1 and len(non_rainy) > 1:
            corr, p = pearsonr(df['precipitation'], df['total_production_daily'])
        else:
            corr, p = None, None
    except Exception as e:
        logging.warning(f"Correlation error: {e}")
        corr, p = None, None
    return corr, p

# --- Validation ---
def validate_data(df, strict=False):
    errors = []

    if (df['total_production_daily'] < 0).any():
        errors.append("Negative production values.")
    if ((df['equipment_utilization'] < 0) | (df['equipment_utilization'] > 100)).any():
        errors.append("Equipment utilization out of range.")
    if df['precipitation'].isnull().any() or df['temp_mean'].isnull().any():
        errors.append("Missing weather data.")

    if errors:
        for err in errors:
            logging.warning(err)
        if strict:
            raise ValueError("Data validation failed:\n" + "\n".join(errors))
        else:
            print("Warning only (non-strict mode):\n" + "\n".join(errors))
    else:
        print("Data validation passed.")
    return True

# --- Forecast ---
def forecast_next_day_production(df):
    warnings.filterwarnings("ignore")
    df_sorted = df.sort_values('date')
    ts = df_sorted.set_index('date')['total_production_daily'].dropna()

    if len(ts) < 10:
        print("Not enough data to forecast.")
        return None

    model = ARIMA(ts, order=(1, 1, 1))
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=1)
    next_day = pd.to_datetime(ts.index[-1]) + pd.Timedelta(days=1)
    print(f"Forecast {next_day.date()}: {forecast.values[0]:.2f} tons")
    return forecast.values[0]

# --- ClickHouse Loader ---
def load_into_clickhouse(df):
    df = df.fillna(0)
    client = clickhouse_connect.get_client(host='clickhouse', port=8123, user='default', password='clickhousepwd')
    client.command("DROP TABLE IF EXISTS daily_production_metrics")
    client.command("""
        CREATE TABLE daily_production_metrics (
            date Date,
            total_production_daily Float64,
            average_quality_grade Float64,
            equipment_utilization Float64,
            fuel_efficiency Float64,
            temperature_mean Float32,
            precipitation Float32
        ) ENGINE = MergeTree() ORDER BY date
    """)
    df_ch = df.rename(columns={'temp_mean': 'temperature_mean'})
    client.insert_df('daily_production_metrics', df_ch[[
        'date', 'total_production_daily', 'average_quality_grade',
        'equipment_utilization', 'fuel_efficiency', 'temperature_mean', 'precipitation'
    ]])

# --- MySQL Wait ---
def wait_for_mysql(host='mysql', user='root', password='rootpwd', db='coal_mining', retries=10, delay=3):
    for attempt in range(retries):
        try:
            print(f"Connecting to MySQL (attempt {attempt + 1})")
            conn = pymysql.connect(host=host, user=user, password=password, db=db)
            return conn
        except Exception as e:
            print(f"Failed: {e}")
            time.sleep(delay)
    raise Exception("MySQL connection failed after retries.")

# --- ETL Runner ---
def run_etl():
    mysql_conn = wait_for_mysql()
    try:
        print("Fetching production logs...")
        prod = fetch_production_logs(mysql_conn)

        print("Fetching sensor data...")
        sensors = fetch_sensors_data()

        print("Computing metrics...")
        metrics = compute_metrics(prod, sensors)

        print("Enriching with weather...")
        enriched = enrich_with_weather(metrics)

        print("Computing weather impact...")
        corr, p = compute_weather_impact(enriched)

        print("Validating data...")
        validate_data(enriched, strict=False)

        print("Loading into ClickHouse...")
        load_into_clickhouse(enriched)

        print("Forecasting...")
        forecast = forecast_next_day_production(enriched)

        print("ETL process complete.")
        return enriched, corr, p, forecast
    finally:
        mysql_conn.close()
