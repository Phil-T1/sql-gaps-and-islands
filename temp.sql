-- Create new table to store the sensor events aggreegated via the CTEs
CREATE OR REPLACE TABLE { event_table_name } AS

    -- Create a row number for each sensor_id in ascending order of timestamp
    WITH sensor_data_with_row_number AS (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY sensor_id
                ORDER BY
                    timestamp
            ) AS row_id
        FROM
            { data_table_name }
    ),
    -- Create an island_id for each sensor_id where the sensor_value is 1
    sensor_data_with_island_id AS (
        SELECT
            *,
            row_id - ROW_NUMBER() OVER (
                PARTITION BY sensor_id
                ORDER BY
                    timestamp
            ) AS island_id
        FROM
            sensor_data_with_row_number
        WHERE
            sensor_value = 1
    ),
    -- Aggregate the sensor data by sensor_id and island_id to get the
    -- event_start_timestamp, event_end_timestamp and event_duration
    sensor_data_event_agg AS (
        SELECT
            sensor_id,
            MIN(timestamp) AS event_start_timestamp,
            MAX(timestamp) AS event_end_timestamp,
            event_end_timestamp - event_start_timestamp AS event_duration
        FROM
            sensor_data_with_island_id
        GROUP BY
            sensor_id,
            island_id
        ORDER BY
            sensor_id,
            event_start_timestamp
    )

-- Populate the {event_table_name} table with the aggregated data from final CTE
SELECT * FROM sensor_data_event_agg;