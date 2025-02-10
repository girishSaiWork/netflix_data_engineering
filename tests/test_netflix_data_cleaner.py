import unittest
from pyspark.sql.functions import *
from src.jobs.netflix_data_cleaner import *  # Import all your functions
from chispa import Chispa
from chispa.dataframe_comparer import assert_df_equality

class NetflixDataCleaningTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark_session()
        cls.chispa = Chispa()
        cls.test_file = "./test_data/test_netflix_titles.csv"
        cls.valid_countries_file = "./data/valid_countries.txt"
        cls.lang_map_file = "./data/countries_languages.json"

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_load_and_analyze_nulls(self):
        df, total_rows = load_and_analyze_nulls(self.spark, self.test_file)
        self.assertEqual(df.count(), 6)  # Check correct number of rows
        self.assertEqual(total_rows, 6) # Check total rows is correct
        # ... (Add more assertions for each column's null count as needed)

    def test_clean_null_values(self):
        df, _ = load_and_analyze_nulls(self.spark, self.test_file)
        cleaned_df = clean_null_values(df, df.count())  # Pass df.count for total rows

        # Define expected DataFrame (after null filling and dropping)
        expected_data = [
            ("s1", "Movie", "The Great Hack", "Karim Amer", "Jehane Noujaim", "United States", "July 24, 2020", 2020, "TV-MA", "113 min", "Documentaries", "A documentary about data exploitation."),
            ("s2", "TV Show", "The Umbrella Academy", "Unknown", "Ellen Page", "United States", "July 31, 2020", None, "TV-14", "2 Seasons", "Action & Adventure", "A superhero series."),
            ("s4", "Movie", "Invalid Country Movie", "Some Director", "Some Cast", "Unknown", "Jan 1, 2023", 2023, "PG-13", "95 min", "Dramas", "A movie with an invalid country."),
            ("s6", "Movie", "Movie with multiple countries", "Director 1", "Cast 1", "USA, Canada", "Dec 10, 2022", 2023, "PG", "120 min", "Action", "A movie with multiple countries")
        ]
        expected_df = self.spark.createDataFrame(expected_data, df.columns)
        assert_df_equality(cleaned_df, expected_df, ignore_nullable=True, ignore_column_order=True)

    def test_process_show_types(self):
        df, _ = load_and_analyze_nulls(self.spark, self.test_file)
        cleaned_df = clean_null_values(df, df.count())
        show_type_df = process_show_types(cleaned_df)

        expected_data = [
            ("s1", "Movie", "The Great Hack", "Karim Amer", "Jehane Noujaim", "United States", "July 24, 2020", 2020, "TV-MA", "113 min", "Documentaries", "A documentary about data exploitation.", "Local", None),
            ("s2", "TV Show", "The Umbrella Academy", "Unknown", "Ellen Page", "United States", "July 31, 2020", None, "TV-14", "2 Seasons", "Action & Adventure", "A superhero series.", None, "Local"),
            ("s4", "Movie", "Invalid Country Movie", "Some Director", "Some Cast", "Unknown", "Jan 1, 2023", 2023, "PG-13", "95 min", "Dramas", "A movie with an invalid country.", "Local", None),
            ("s6", "Movie", "Movie with multiple countries", "Director 1", "Cast 1", "USA, Canada", "Dec 10, 2022", 2023, "PG", "120 min", "Action", "A movie with multiple countries", "Global", None)
        ]
        expected_df = self.spark.createDataFrame(expected_data, show_type_df.columns)
        assert_df_equality(show_type_df, expected_df, ignore_nullable=True, ignore_column_order=True)

    def test_country_cleaning(self):

        df, _ = load_and_analyze_nulls(self.spark, self.test_file)
        cleaned_df = clean_null_values(df, df.count())
        show_type_df = process_show_types(cleaned_df)
        country_split_df = (show_type_df.withColumn("country", regexp_replace(col('country'), r'^\s*,\s*', ''))
                            .withColumn("countries", split("country", r",\s*")))
        country_explode_df = country_split_df.withColumn("country", explode_outer(country_split_df["countries"]))
        valid_countries_df = self.spark.read.format('csv').option('header', 'true').option('inferSchema', True).load(self.valid_countries_file)
        # 2. Clean the country column
        cleaned_df = country_explode_df.join(valid_countries_df, country_explode_df["country"] == valid_countries_df["country"], "left_anti")
        invalid_countries = list(set([row.country for row in cleaned_df.collect()]))
        country_clean_df = country_explode_df.withColumn(
            'country_clean',
            when(col("country").isin(invalid_countries), array(lit('Unknown'))).otherwise(col('countries'))
        ).drop('countries')

        expected_data = [
            ("s1", "Movie", "The Great Hack", "Karim Amer", "Jehane Noujaim", "United States", "July 24, 2020", 2020, "TV-MA", "113 min", "Documentaries", "A documentary about data exploitation.", "Local", None, ["United States"]),
            ("s2", "TV Show", "The Umbrella Academy", "Unknown", "Ellen Page", "United States", "July 31, 2020", None, "TV-14", "2 Seasons", "Action & Adventure", "A superhero series.", None, "Local", ["United States"]),
            ("s4", "Movie", "Invalid Country Movie", "Some Director", "Some Cast", "Unknown", "Jan 1, 2023", 2023, "PG-13", "95 min", "Dramas", "A movie with an invalid country.", "Local", None, ["Unknown"]),
            ("s6", "Movie", "Movie with multiple countries", "Director 1", "Cast 1", "USA, Canada", "Dec 10, 2022", 2023, "PG", "120 min", "Action", "A movie with multiple countries", "Global", None, ["USA", "Canada"])
        ]
        expected_df = self.spark.createDataFrame(expected_data, country_clean_df.columns)
        assert_df_equality(country_clean_df, expected_df, ignore_nullable=True, ignore_column_order=True)

    def test_language_mapping(self):
        df, _ = load_and_analyze_nulls(self.spark, self.test_file)
        cleaned_df = clean_null_values(df, df.count())
        show_type_df = process_show_types(cleaned_df)
        country_split_df = (show_type_df.withColumn("country", regexp_replace(col('country'), r'^\s*,\s*', ''))
                            .withColumn("countries", split("country", r",\s*")))
        country_explode_df = country_split_df.withColumn("country", explode_outer(country_split_df["countries"]))
        valid_countries_df = self.spark.read.format('csv').option('header', 'true').option('inferSchema', True).load(self.valid_countries_file)
        # 2. Clean the country column
        cleaned_df = country_explode_df.join(valid_countries_df, country_explode_df["country"] == valid_countries_df["country"], "left_anti")
        invalid_countries = list(set([row.country for row in cleaned_df.collect()]))
        country_clean_df = country_explode_df.withColumn(
            'country_clean',
            when(col("country").isin(invalid_countries), array(lit('Unknown'))).otherwise(col('countries'))
        ).drop('countries')
        # Step 1: Read the JSON file into a DataFrame
        lang_map_df = self.spark.read.option("multiline", "true").json(self.lang_map_file)
        join_df = lang_map_df.join(country_clean_df, 'country', "inner")
        df_exploded = join_df.withColumn("Language", explode("Languages"))
        # Group by country_clean and collect the languages into a list
        languages_df = df_exploded.groupBy("country_clean").agg(collect_list("Language").alias("Languages_array"))
        # Distinct the languages in Languages_array
        languages_df = languages_df.withColumn("Languages_array", array_distinct(col("Languages_array")))
        # Join back with the original DataFrame
        result_df = join_df.join(languages_df, "country_clean", "left")
        result_df_cols_dropped = result_df.drop('Country','Languages').withColumnRenamed('country_clean','released_countries').withColumnRenamed('Languages_array','released_languages').dropDuplicates()
        # Define the expected DataFrame
        expected_data = [
            ("s1", "Movie", "The Great Hack", "Karim Amer", "Jehane Noujaim", "United States", "July 24, 2020", 2020, "TV-MA", "113 min", "Documentaries", "A documentary about data exploitation.", "Local", None, ["United States"], ["English"]),
            ("s2", "TV Show", "The Umbrella Academy", "Unknown", "Ellen Page", "United States", "July 31, 2020", None, "TV-14", "2 Seasons", "Action & Adventure", "A superhero series.", None, "Local", ["United States"], ["English"]),
            ("s4", "Movie", "Invalid Country Movie", "Some Director", "Some Cast", "Unknown", "Jan 1, 2023", 2023, "PG-13", "95 min", "Dramas", "A movie with an invalid country.", "Local", None, ["Unknown"], ["Unknown"]),
            ("s6", "Movie", "Movie with multiple countries", "Director 1", "Cast 1", "USA, Canada", "Dec 10, 2022", 2023, "PG", "120 min", "Action", "A movie with multiple countries", "Global", None, ["USA", "Canada"], ["English", "French"])
        ]
        expected_df = self.spark.createDataFrame(expected_data, result_df_cols_dropped.columns)
        assert_df_equality(result_df_cols_dropped, expected_df, ignore_nullable=True, ignore_column_order=True)

    def test_duration_handling(self):
        df, _ = load_and_analyze_nulls(self.spark, self.test_file)
        cleaned_df = clean_null_values(df, df.count())
        show_type_df = process_show_types(cleaned_df)
        country_split_df = (show_type_df.withColumn("country", regexp_replace(col('country'), r'^\s*,\s*', ''))
                            .withColumn("countries", split("country", r",\s*")))
        country_explode_df = country_split_df.withColumn("country", explode_outer(country_split_df["countries"]))
        valid_countries_df = self.spark.read.format('csv').option('header', 'true').option('inferSchema', True).load(self.valid_countries_file)
        # 2. Clean the country column
        cleaned_df = country_explode_df.join(valid_countries_df, country_explode_df["country"] == valid_countries_df["country"], "left_anti")
        invalid_countries = list(set([row.country for row in cleaned_df.collect()]))
        country_clean_df = country_explode_df.withColumn(
            'country_clean',
            when(col("country").isin(invalid_countries), array(lit('Unknown'))).otherwise(col('countries'))
        ).drop('countries')
        # Step 1: Read the JSON file into a DataFrame
        lang_map_df = self.spark.read.option("multiline", "true").json(self.lang_map_file)
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

        expected_data = [
            ("s1", "Movie", "The Great Hack", "Karim Amer", "Jehane Noujaim", "United States", "July 24, 2020", 2020, "TV-MA", 113, None, "Documentaries", "A documentary about data exploitation.", "Local", None, ["United States"], ["English"]),
            ("s2", "TV Show", "The Umbrella Academy", "Unknown", "Ellen Page", "United States", "July 31, 2020", None, "TV-14", None, 2, "Action & Adventure", "A superhero series.", None, "Local", ["United States"], ["English"]),
            ("s4", "Movie", "Invalid Country Movie", "Some Director", "Some Cast", "Unknown", "Jan 1, 2023", 2023, "PG-13", 95, None, "Dramas", "A movie with an invalid country.", "Local", None, ["Unknown"], ["Unknown"]),
            ("s6", "Movie", "Movie with multiple countries", "Director 1", "Cast 1", "USA, Canada", "Dec 10, 2022", 2023, "PG", 120, None, "Action", "A movie with multiple countries", "Global", None, ["USA", "Canada"], ["English", "French"])
        ]
        expected_df = self.spark.createDataFrame(expected_data, durations_df.columns)
        assert_df_equality(durations_df, expected_df, ignore_nullable=True, ignore_column_order=True)

if __name__ == "__main__":
    unittest.main()