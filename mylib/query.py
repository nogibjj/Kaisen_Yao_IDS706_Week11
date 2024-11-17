from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when


def query_transform():
    """
    Run a predefined SQL query on a Spark DataFrame.

    Returns:
        DataFrame: Result of the SQL query.
    """
    spark = SparkSession.builder.appName("Query").getOrCreate()

    # Sample data setup (replace with actual table registration if needed)
    # For this example, assume 'fifa_countries_audience' is already registered as a table.

    query = """
        SELECT confederation, 
               COUNT(*) as country_count, 
               ROUND(AVG(gdp_weighted_share), 2) as avg_gdp_share
        FROM fifa_countries_audience
        GROUP BY confederation
        ORDER BY country_count DESC
        """
    query_result = spark.sql(query)
    return query_result


def example_transform(df):
    """
    Transform FIFA data to add region categories.

    Args:
        df (DataFrame): Input Spark DataFrame.

    Returns:
        DataFrame: Transformed DataFrame with 'region_category' column added.
    """
    conditions = [
        (col("confederation") == "AFC") | (col("confederation") == "UEFA"),
        (col("confederation") == "CONCACAF") | (col("confederation") == "CONMEBOL"),
    ]
    categories = ["Eurasia", "Americas"]

    df = df.withColumn(
        "region_category",
        when(conditions[0], categories[0])
        .when(conditions[1], categories[1])
        .otherwise("Other"),
    )

    # Print the transformed DataFrame
    df.show()
    return df


if __name__ == "__main__":
    # Execute the query and get the result
    query_result = query_transform()

    # Transform the query result using `example_transform`
    transformed_result = example_transform(query_result)

    # Show the final transformed result
    transformed_result.show()
