import polars as pl
import json

def read_json_to_polars(file_path):
    """
    Reads a JSON file of container records into a Polars DataFrame.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        polars.DataFrame: A DataFrame containing the parsed data.
    """
    # Load JSON data from file
    with open(file_path, 'r') as f:
        data = json.load(f)

    # Extract the 'body' field and parse it into a list of dictionaries
    records = [json.loads(record['body']) for record in data['Records']]

    # Convert the list of dictionaries into a Polars DataFrame
    df = pl.DataFrame(records)

    return df

def check_if_enough_capacity_in_location(df_location, df_container):
    """
    Check if there is enough capacity in the location to store the containers.
    If not, give an overview of depots that are over capacity and with how much over capacity.

    """
    volume_destination_depot = df_container['destination_depot'].value_counts()

    # TODO: rewrite names of columns to something more meaningful than count_right
    df_combined = df_location.join(volume_destination_depot,
                                   left_on="location",   # Column in df_location
                                   right_on="destination_depot",  # Column in df_containers
                                   how="full"           # Full outer join
                                   )

    df_combined = df_combined.with_columns(
        (pl.col("count_right") - pl.col("count")).alias("deficit")
    )

    # Filter the rows where there is a deficit
    df_with_deficit = df_combined.filter(pl.col("deficit") > 0)

    return df_with_deficit[['location', 'deficit']]

if __name__ == '__main__':
    file_path = '../data/dummydata.json'
    df_container = read_json_to_polars(file_path)
    df_location = pl.read_csv('../data/location.csv')
    df_problem_depots = check_if_enough_capacity_in_location(df_location, df_container)

