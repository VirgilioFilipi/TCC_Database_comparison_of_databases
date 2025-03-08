class QueryDatabase:
    """Classe para montar as query."""

    @staticmethod
    def query_1_year_a():
        query = f"""
            SELECT event_timestamp, temperature, sensor_name
            FROM sensor_data
            WHERE event_timestamp >= '2023-01-01 00:00:00' 
            AND event_timestamp < '2024-01-01 00:00:00'
            AND sensor_name = 'Sensor A';
        """
        return query

    @staticmethod
    def query_1_day_full():
        query = f"""
            SELECT event_timestamp, temperature, sensor_name
            FROM sensor_data
            WHERE event_timestamp >= '2023-01-02 00:00:00' 
            AND event_timestamp < '2023-01-03 00:00:00';
        """
        return query

    @staticmethod
    def query_group_mean_week_b():
        query = f"""
        SELECT 
            YEARWEEK(event_timestamp, 1) AS week_interval,
            AVG(temperature) AS avg_temp
            FROM sensor_data
            WHERE sensor_name = 'Sensor B'
            AND event_timestamp >= '2023-01-02 00:00:00' 
            AND event_timestamp < '2023-06-01 00:00:00'
            GROUP BY week_interval
            ORDER BY week_interval;
        """
        return query

    @staticmethod
    def query_group_sum_month_a():
        query = f"""
            SELECT 
                DATE_FORMAT(event_timestamp, '%Y-%m') AS month_start, 
                SUM(temperature) AS sum_temperature
            FROM sensor_data
            WHERE sensor_name = 'Sensor A'
            GROUP BY month_start
            ORDER BY month_start;
        """
        return query

    @staticmethod
    def query_group_mean_min_a():
        query = f"""
        SELECT 
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_timestamp) / (15 * 60)) * (15 * 60)) AS interval_15min,
            AVG(temperature) AS avg_temp
        FROM sensor_data
        WHERE sensor_name = 'Sensor A'
        GROUP BY interval_15min
        ORDER BY interval_15min;
        """
        return query
    
    @staticmethod
    def query_max_min_days_full():
        query = f"""
            SELECT 
                MAX(temperature) AS max_temp, 
                MIN(temperature) AS min_temp
            FROM sensor_data
            WHERE event_timestamp >= '2023-01-01 00:00:00' 
            AND event_timestamp < '2023-01-10 00:00:00';
        """
        return query
    
    @staticmethod
    def query_count_line_full():
        query = f"""
            SELECT COUNT(*) 
            FROM sensor_data;
        """
        return query


    @staticmethod
    def query_1_year_a_structured():
        query = f"""
            SELECT event_timestamp, temperature, sensor_name
            FROM sensor_data
            WHERE year_number = 2023
            AND sensor_name = 'Sensor A';
        """
        return query

    @staticmethod
    def query_1_day_full_structured():
        query = f"""
            SELECT event_timestamp, temperature, sensor_name
            FROM sensor_data
            WHERE year_number = 2023
            AND event_timestamp BETWEEN '2023-01-02 00:00:00' AND '2023-01-02 23:59:59';

        """
        return query

    @staticmethod
    def query_group_mean_week_b_structured():
        query = f"""
        SELECT 
            YEARWEEK(event_timestamp, 1) AS week_interval,
            AVG(temperature) AS avg_temp
        FROM sensor_data
        WHERE year_number = 2023
        AND sensor_name = 'Sensor B'
        AND event_timestamp BETWEEN '2023-01-02 00:00:00' AND '2023-05-31 23:59:59'

        GROUP BY week_interval
        ORDER BY week_interval;
        """
        return query

    @staticmethod
    def query_group_sum_month_a_structured():
        query = f"""
        SELECT 
            YEAR(event_timestamp) AS year,
            MONTH(event_timestamp) AS month, 
            SUM(temperature) AS sum_temperature
        FROM sensor_data
        WHERE sensor_name = 'Sensor A'
        GROUP BY year, month
        ORDER BY year, month;
        """
        return query

    @staticmethod
    def query_group_mean_min_a_structured():
        query = f"""
        SELECT 
            FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(event_timestamp) / (15 * 60)) * (15 * 60)) AS interval_15min,
            AVG(temperature) AS avg_temp
        FROM sensor_data
        WHERE sensor_name = 'Sensor A'
        GROUP BY interval_15min
        ORDER BY interval_15min;
        """
        return query
    
    @staticmethod
    def query_max_min_days_full_structured():
        query = f"""
            SELECT 
                MAX(temperature) AS max_temp, 
                MIN(temperature) AS min_temp
            FROM sensor_data
            WHERE year_number = 2023
            AND event_timestamp BETWEEN '2023-01-01 00:00:00' AND '2023-01-09 23:59:59';

        """
        return query
    
    @staticmethod
    def query_count_line_full_structured():
        query = f"""
            SELECT COUNT(*) 
            FROM sensor_data;
        """
        return query

    @staticmethod
    def query_1_year_a_influx():
        query = """
            from(bucket: "influx_bucket")
            |> range(start: 2023-01-01T00:00:00Z, stop: 2024-01-01T00:00:00Z)
            |> filter(fn: (r) => r._measurement == "sensor_data")
            |> filter(fn: (r) => r._field == "temperature")
            |> filter(fn: (r) => r.sensor_name == "Sensor A")
            |> keep(columns: ["_time", "_value", "sensor_name"])
            |> yield(name: "complete_data")
        """
        return query

    @staticmethod
    def query_1_day_full_influx():
        query = """
            from(bucket: "influx_bucket")
            |> range(start: 2023-01-02T00:00:00Z, stop: 2023-01-03T00:00:00Z)
            |> filter(fn: (r) => r._measurement == "sensor_data")
            |> filter(fn: (r) => r._field == "temperature")
            |> keep(columns: ["_time", "_value", "sensor_name"])
            |> yield(name: "complete_data")
        """
        return query

    @staticmethod
    def query_group_mean_week_b_influx():
        query = """
        from(bucket: "influx_bucket")
        |> range(start: 2023-01-02T00:00:00Z, stop: 2023-06-01T00:00:00Z)  
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.sensor_name == "Sensor B")
        |> filter(fn: (r) => r._field == "temperature")
        |> keep(columns: ["_time", "_value", "sensor_name"])

        |> group()
        |> aggregateWindow(every: 1w, fn: mean, createEmpty: false) 

        """
        return query
    
    @staticmethod
    def query_group_sum_month_a_influx():
        query = """
        from(bucket: "influx_bucket")
        |> range(start: 0)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.sensor_name == "Sensor A")
        |> filter(fn: (r) => r._field == "temperature")
        |> keep(columns: ["_time", "_value", "sensor_name"])
        |> group() 
        |> aggregateWindow(every: 1mo, fn: sum, createEmpty: false)
        """
        return query

    @staticmethod
    def query_group_mean_min_a_influx():
        query = """
        from(bucket: "influx_bucket")
        |> range(start: 0)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r.sensor_name == "Sensor A")
        |> filter(fn: (r) => r._field == "temperature")
        |> aggregateWindow(every: 15m, fn: mean, createEmpty: false) // Agrupar e calcular média
        |> group(columns: ["_time"])  // Agrupar por intervalo de tempo
        |> map(fn: (r) => ({ _time: r._time, _value: r._value }))
        """
        return query
    
    @staticmethod
    def query_max_min_days_full_influx():
        query = """
        from(bucket: "influx_bucket")
        |> range(start: 2023-01-01T00:00:00Z, stop: 2023-01-10T00:00:00Z)
        |> filter(fn: (r) => r._measurement == "sensor_data")
        |> filter(fn: (r) => r._field == "temperature")
        |> keep(columns: ["_time", "_value", "sensor_name"])
        |> reduce(
            identity: {max: float(v: "-inf"), min: float(v: "inf")},
            fn: (r, accumulator) => ({
                max: if r._value > accumulator.max then r._value else accumulator.max,
                min: if r._value < accumulator.min then r._value else accumulator.min
            })
        )
        """
        return query

    @staticmethod
    def query_count_line_full_influx():
        query = """
            from(bucket: "influx_bucket")
            |> range(start: 0)
            |> filter(fn: (r) => r._measurement == "sensor_data")
            |> count(column: "_value")  
            |> yield(name: "row_count")
        """
        return query

