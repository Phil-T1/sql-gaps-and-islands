from datetime import datetime
import pandas as pd
import random

# Define the columns for the sensor data
COLS = ['timestamp', 'sensor_id', 'sensor_value']

def generate_continuous_sensors_event_data(
    start_datetime: datetime = datetime(2024, 12, 12, 0, 0, 0),
    end_datetime: datetime = datetime(2024, 12, 13, 0, 0, 0),
    sensors: int = 3,
    sensor_sample_rate_secs: int = 5,
    min_events: int = 1,
    max_events: int = 5,
    min_event_duration_secs: int = 60,
    max_event_duration_secs: int = 3600 * 3,
    output_file_name: str = 'sensor_data_continuous.csv'
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

    Returns:
        None: Data is saved directly to a CSV file
    """
    
    # Create empty list to store sensor data dataframes
    df_list = []
    
    # Create data for each sensor
    for sensor in range(1, sensors + 1):
        df_list.append(
            get_sensor_event_data_df(
                sensor_num = sensor,
                start_datetime = start_datetime,
                end_datetime = end_datetime,
                sensor_sample_rate_secs = sensor_sample_rate_secs,
                min_events = min_events,
                max_events = max_events,
                min_event_duration_secs = min_event_duration_secs,
                max_event_duration_secs = max_event_duration_secs
                )
            )
        
    # Concatenate the dataframes for all sensors
    sensor_data_df = pd.concat(df_list)
        
    # Order data by timestamp, sensor_id, and reset the index
    sensor_data_df = sensor_data_df.sort_values(['timestamp', 'sensor_id']).reset_index(drop=True)
        
    # Write to CSV file
    sensor_data_df.to_csv(output_file_name, index=False)
        
        
def get_sensor_event_data_df(
    sensor_num: int,
    start_datetime: datetime,
    end_datetime: datetime,
    sensor_sample_rate_secs: int,
    min_events: int,
    max_events: int,
    min_event_duration_secs: int,
    max_event_duration_secs: int
    ) -> pd.DataFrame:
    """Generate synthetic sensor event data for a single sensor.
    
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
        pd.DataFrame: Sensor event data with timestamp, sensor_id, and sensor_value columns
    """
        
    # Create array of timestamps using pandas
    timestamps = pd.date_range(start=start_datetime, end=end_datetime, freq=f'{sensor_sample_rate_secs}s')

    # Randomise a number of events for the sensor
    num_events = random.randint(min_events, max_events)
    
    # Create empty array of sensor values defaulting to 0 with the same length as the timestamps
    sensor_values = [0] * len(timestamps)
    
    for _ in range(num_events):
        # Randomise the duration of the event
        event_duration = random.randint(min_event_duration_secs, max_event_duration_secs)
        
        # Randomise the start time of the event
        start_time = random.randint(0, len(timestamps) - event_duration)
        
        # Create a list of 1s with length equal to the event duration
        event_values = [1] * (event_duration // sensor_sample_rate_secs)
        
        # Set the array values to 1 for each timestamp within the event duration
        sensor_values[start_time:start_time + (event_duration // sensor_sample_rate_secs)] = event_values
    
    # Create an array of sensor ID values
    sensor_ids = [sensor_num] * len(timestamps)
    
    # Create a dictionary of the sensor data
    sensor_test_data_dict = {
        'timestamp': timestamps,
        'sensor_id': sensor_ids,
        'sensor_value': sensor_values
        }
    
    # Create a dataframe from the dictionary
    return pd.DataFrame(sensor_test_data_dict)

if __name__ == '__main__':
    generate_continuous_sensors_event_data()
