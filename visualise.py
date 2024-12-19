import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import matplotlib.dates as mdates


def plot_sensor_data(df: pd.DataFrame) -> None:
    """Plot the sensor data using steps to show state changes over time.

    Args:
        df (pd.DataFrame): DataFrame containing sensor data with columns:
            - timestamp: Datetime of the reading
            - sensor_id: ID of the sensor
            - value: State of the sensor (0=OFF, 1=ON)
    """
    # Convert timestamp to datetime with specific format
    df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S")

    # Create the plot
    plt.figure(figsize=(15, 8))
    sns.set_style("whitegrid")

    # Plot each sensor's data using steps
    for sensor in df["sensor_id"].unique():
        sensor_data = df[df["sensor_id"] == sensor]
        plt.step(
            sensor_data["timestamp"],
            sensor_data["sensor_value"],
            label=f"Sensor {sensor}",
            where="post",  # Use 'post' to align steps with the timestamp
            linewidth=2,
        )

    plt.title("Sensor States Over Time", fontsize=14)
    plt.xlabel("Hour of Day", fontsize=12)
    plt.ylabel("State (0=OFF, 1=ON)", fontsize=12)

    # Format x-axis to show hours
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter("%H:00"))
    plt.gca().xaxis.set_major_locator(
        mdates.HourLocator(interval=2)
    )  # Show every 2 hours

    plt.ylim(-0.1, 1.1)  # Add some padding to the y-axis
    plt.legend(loc="upper right")
    plt.grid(True, which="both", linestyle="--", alpha=0.7)
    plt.tight_layout()
