from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def create_spark_session():
    """Create and return a Spark session"""
    spark = SparkSession.builder.appName("Netflix Data Analysis").getOrCreate()
    print(f"Created Spark session with version: {spark.version}")
    return spark

def load_and_analyze_nulls(spark, file_path):
    """Load Netflix data and analyze null values"""
    print("\n=== Loading Netflix Dataset ===")
    netflix_data = spark.read.format('csv').option('header', 'true').option('inferSchema', True).load(file_path)
    
    total_rows = netflix_data.count()
    print(f"\nTotal rows in dataset: {total_rows:,}")
    
    print("\n=== Analyzing Null Values (Before Cleaning) ===")
    for data_col in netflix_data.columns:
        null_count = netflix_data.filter(col(data_col).isNull()).count()
        percentage_null = (null_count / total_rows) * 100
        print(f"Column '{data_col}': {null_count:,} nulls ({percentage_null:.2f}%)")
    
    return netflix_data, total_rows

def clean_null_values(netflix_data, total_rows):
    """Fill null values and drop remaining null records"""
    print("\n=== Filling Null Values ===")
    netflix_data_null_fill = netflix_data.na.fill({
        'director': 'Unknown',
        'cast': 'Unknown',
        'country': 'Unknown'
    })
    
    print("\n=== Analyzing Null Values (After Filling) ===")
    for data_col in netflix_data.columns:
        null_count = netflix_data_null_fill.filter(col(data_col).isNull()).count()
        percentage_null = (null_count / total_rows) * 100
        print(f"Column '{data_col}': {percentage_null:.2f}%")
    
    netflix_nulls_drop = netflix_data_null_fill.na.drop()
    print(f"\nRows after dropping remaining nulls: {netflix_nulls_drop.count():,}")
    
    return netflix_nulls_drop

def process_show_types(df):
    """Process and categorize show types"""
    print("\n=== Processing Show Types ===")
    show_movie_type_df = df.withColumn(
        "movie_type",
        when(col("type") == "Movie", 
             when(col("listed_in").contains("International"), "Global")
             .otherwise("Local"))
    ).withColumn(
        "show_type",
        when(col("type") == "TV Show", 
             when(col("listed_in").contains("International"), "Global")
             .otherwise("Local"))
    )
    return show_movie_type_df

def main():
    """Main function to orchestrate the Netflix data cleaning process"""
    # Initialize constants
    FILE_PATH = "../data/netflix_titles.csv"
    VALID_COUNTRY_FILE_PATH = "../data/valid_countries.txt"
    LANG_MAP_FILE_PATH = "../data/countries_languages.json"
    OUTPUT_PATH = '../target_data'
    
    # Create Spark session
    spark = create_spark_session()
    
    try:
        # Load and process data
        netflix_data, total_rows = load_and_analyze_nulls(spark, FILE_PATH)
        
        # Clean null values
        cleaned_df = clean_null_values(netflix_data, total_rows)
        
        # Process show types
        show_type_df = process_show_types(cleaned_df)
        
        # Continue with existing transformations...
        genre_df = show_type_df.withColumn("genres", split(show_type_df["listed_in"], ",\\s*")).drop('listed_in')  # Split by comma and whitespace
        tsa_columns_gen_df = genre_df.withColumn('added_date', to_date(genre_df['date_added'], "MMMM d, yyyy"))
        # Extract year, month, date, and day_of_week
        tsa_columns_gen_df = tsa_columns_gen_df.withColumn("year", date_format(col("added_date"), "yyyy")) \
               .withColumn("month", date_format(col("added_date"), "MMMM")) \
               .withColumn("day", date_format(col("added_date"), "d")) \
               .withColumn("day_of_week", date_format(col("added_date"), "EEEE"))
        country_split_df = (tsa_columns_gen_df.withColumn("country", regexp_replace(col('country'), r'^\s*,\s*', ''))
                            .withColumn("countries", split("country", r",\s*")))
        country_explode_df = country_split_df.withColumn("country", explode_outer(country_split_df["countries"]))
        valid_countries_df = spark.read.format('csv').option('header', 'true').option('inferSchema', True).load(VALID_COUNTRY_FILE_PATH)
        # 2. Clean the country column
        cleaned_df = country_explode_df.join(valid_countries_df, country_explode_df["country"] == valid_countries_df["country"], "left_anti")
        invalid_countries = list(set([row.country for row in cleaned_df.collect()]))
        country_clean_df = country_explode_df.withColumn(
            'country_clean',
            when(col("country").isin(invalid_countries), array(lit('Unknown'))).otherwise(col('countries'))
        ).drop('countries')
        # Step 1: Read the JSON file into a DataFrame
        lang_map_df = spark.read.option("multiline", "true").json(LANG_MAP_FILE_PATH)
        join_df = lang_map_df.join(country_clean_df, 'country', "inner")
        df_exploded = join_df.withColumn("Language", explode("Languages"))
        # Group by country_clean and collect the languages into a list
        languages_df = df_exploded.groupBy("country_clean").agg(collect_list("Language").alias("Languages_array"))
        # Distinct the languages in Languages_array
        languages_df = languages_df.withColumn("Languages_array", array_distinct(col("Languages_array")))
        # Join back with the original DataFrame
        result_df = join_df.join(languages_df, "country_clean", "left")
        result_df_cols_dropped = result_df.drop('Country','Languages').withColumnRenamed('country_clean','released_countries').withColumnRenamed('Languages_array','released_languages').dropDuplicates()
        result_df_cols_dropped=result_df_cols_dropped.withColumn('release_year', col('release_year').cast('Integer'))
        # Transform 'duration' column for movies and TV shows
        durations_df = (
            result_df_cols_dropped
            .withColumn(
                'movie_duration',
                when(col('type') == 'Movie', regexp_replace(col('duration'), ' min', '').cast('Integer'))
                .otherwise(lit(None))  # Use None for NULL values
            )
            .withColumn(
                'seasons',
                when(col('type') == 'TV Show', regexp_replace(col('duration'), r'\D', '').cast('Integer'))
                .otherwise(lit(None))
            )
        ).drop('duration')
        columns = [
            'show_id',
            'type',
            'title',
            'director',
            'cast',
            'date_added',
            'release_year',
            'rating',
            'movie_duration',
            'seasons',
            'description',
            'released_countries',
            'released_languages',  # Missing comma was added here
            'movie_type',
            'show_type',
            'genres',
            'added_date',
            'year',
            'month',
            'day',
            'day_of_week'
        ]
        netflix_ordered_select = durations_df.select(*columns)
        
        # Save final output
        print("\n=== Saving Cleaned Data ===")
        netflix_ordered_select.write.format('parquet').mode('overwrite').save(OUTPUT_PATH)
        print(f"Data successfully saved to: {OUTPUT_PATH}")
        
        # Display sample of cleaned data
        print("\n=== Sample of Cleaned Data ===")
        netflix_cleaned_data = spark.read.parquet(OUTPUT_PATH)
        netflix_cleaned_data.show(5, truncate=False)
        
    except Exception as e:
        print(f"Error processing Netflix data: {str(e)}")
    finally:
        spark.stop()
        print("\n=== Spark Session Stopped ===")

if __name__ == "__main__":
    main()