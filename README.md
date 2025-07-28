# Coal Mining ETL Dashboard Project for AI Engineer - Synapsis (Febriyeni Susi)

This project provides a complete ETL (Extract, Transform, Load) data pipeline and a dashboard visualization for coal mining operations. 
The goal is to monitor production performance, equipment usage, and analyze the impact of weather on daily coal output using modern open-source tools.

## Features

- Automated ETL pipeline using Python
- Weather data integration from Open-Meteo API
- Metrics calculation: total production, quality grade, equipment utilization, fuel efficiency
- Weather impact analysis via correlation
- Time-series forecasting for next-day production
- Dashboard built using Metabase

## Tech Stack

- **Python** (ETL scripting, forecasting with ARIMA)
- **MySQL** (production logs storage)
- **ClickHouse** (analytics database for dashboard)
- **Metabase** (data visualization & dashboard)
- **Docker** (containerized services)
- **Flask** (ETL trigger interface)

  
## Visualizations in Metabase

The Metabase dashboard includes:
**Line Chart**: Daily coal production trend over time
**Bar Chart**: Comparison of average coal quality across mine locations
**Scatter Plot**: Relationship between rainfall and daily production

## ETL Pipeline Overview

1. **Extract** data from:
   - MySQL (`production_logs`)
   - CSV sensor data
   - Open-Meteo API (weather)

2. **Transform**:
   - Aggregate production & sensor metrics
   - Calculate utilization, fuel efficiency
   - Merge weather data and compute correlation

3. **Load**:
   - Final daily metrics are stored in ClickHouse
   - Available for dashboarding in Metabase

## Forecasting
A time-series forecasting model (ARIMA) is included to predict the next day's production based on historical trends.

