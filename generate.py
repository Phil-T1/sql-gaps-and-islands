from datetime import datetime
import pandas as pd
import random


def generate_continuous_sensors_event_data(
    start_datetime: datetime = datetime(2024, 12, 12, 0, 0, 0),
    end_datetime: datetime = datetime(2024, 12, 13, 0, 0, 0),
    sensors: int = 3,
    sensor_sample_rate_secs: int = 5,
    min_events: int = 1,
    max_events: int = 5,
    min_event_duration_secs: int = 60,
    max_event_duration_secs: int = 3600 * 3,
    output_file_name: str = "sensor_data_continuous.csv",
) -> None:
    """Generate synthetic sensor event data and save it to a CSV file.

    The function creates a time series dataset with multiple sensors, each generating
    random events of varying durations within the specified time range. The data
    is structured with timestamp, sensor_id, and sensor_value columns.

    Args:
        start_datetime (datetime): Start of the data range
        end_datetime (datetime): End of the data range
        sensors (int): Number of sensors generating data
        sensor_sample_rate_secs (int): Time between sensor readings in seconds
        min_events (int): Minimum number of events per sensor
        max_events (int): Maximum number of events per sensor
        min_event_duration_secs (int): Minimum duration of an event in seconds
        max_event_duration_secs (int): Maximum duration of an event in seconds
        output_file_name (str): Name of the output CSV file

    Returns:
        None: Data is saved directly to a CSV file
    """
    # Generate data for all sensors using list comprehension and concatenate
    pd.concat(
        [
            get_continuous_sensor_event_data_df(
                sensor_num=sensor,
                start_datetime=start_datetime,
                end_datetime=end_datetime,
                sensor_sample_rate_secs=sensor_sample_rate_secs,
                min_events=min_events,
                max_events=max_events,
                min_event_duration_secs=min_event_duration_secs,
                max_event_duration_secs=max_event_duration_secs,
            )
            for sensor in range(1, sensors + 1)
        ]
    ).sort_values(["timestamp", "sensor_id"]).reset_index(drop=True).to_csv(
        output_file_name, index=False
    )


def get_continuous_sensor_event_data_df(
    sensor_num: int,
    start_datetime: datetime,
    end_datetime: datetime,
    sensor_sample_rate_secs: int,
    min_events: int,
    max_events: int,
    min_event_duration_secs: int,
    max_event_duration_secs: int,
) -> pd.DataFrame:
    """Generate synthetic sensor event data for a single sensor.

    Creates a DataFrame containing event data for a single sensor over the specified
    time range. Events are randomly generated with varying durations, where a value
    of 1 indicates an event is occurring and 0 indicates no event.

    Args:
        sensor_num (int): Sensor ID
        start_datetime (datetime): Start of the data range
        end_datetime (datetime): End of the data range
        sensor_sample_rate_secs (int): Time between sensor readings in seconds
        min_events (int): Minimum number of events per sensor
        max_events (int): Maximum number of events per sensor
        min_event_duration_secs (int): Minimum duration of an event in seconds
        max_event_duration_secs (int): Maximum duration of an event in seconds

    Returns:
        pd.DataFrame: DataFrame containing sensor event data with columns:
            - timestamp: datetime of the reading
            - sensor_id: identifier of the sensor
            - sensor_value: binary value indicating event occurrence (0 or 1)
    """
    # Generate continuous series of timestamps for sensor_id at given sample rate
    timestamps = pd.date_range(
        start=start_datetime, end=end_datetime, freq=f"{sensor_sample_rate_secs}s"
    )
    total_samples = len(timestamps)

    # Initialize sensor values array with zeros
    sensor_values = pd.Series(0, index=range(total_samples))

    # Generate random events
    for _ in range(random.randint(min_events, max_events)):
        duration_steps = (
            random.randint(min_event_duration_secs, max_event_duration_secs)
            // sensor_sample_rate_secs
        )
        start_idx = random.randint(0, total_samples - duration_steps)
        sensor_values.iloc[start_idx : start_idx + duration_steps] = 1

    # Return DataFrame with sensor data
    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "sensor_id": sensor_num,
            "sensor_value": sensor_values,
        }
    )


# Run this file directly to test the function
if __name__ == "__main__":
    generate_continuous_sensors_event_data()
