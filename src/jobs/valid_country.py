from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize Spark session
spark = SparkSession.builder.appName("ValidCountries").getOrCreate()

# List of valid country names
valid_countries = [
    'Russia', 'Paraguay', 'Senegal', 'Sweden', 'Philippines', 'Singapore', 'Malaysia', 'Turkey', 'Malawi', 'Iraq',
    'Germany', 'Cambodia', 'Afghanistan', 'Jordan', 'Sudan', 'France', 'Greece', 'Sri Lanka', 'Taiwan', 'Algeria',
    'Slovakia', 'Argentina', 'Belgium', 'Angola', 'Qatar', 'Ecuador', 'Albania', 'Finland', 'Ghana', 'Nicaragua',
    'Peru', 'United States', 'India', 'China', 'Bahamas', 'Belarus', 'Kuwait', 'Malta', 'Somalia', 'Chile',
    'Puerto Rico', 'Cayman Islands', 'Soviet Union', 'Croatia', 'Nigeria', 'Italy', 'Lithuania', 'Norway', 'Spain',
    'Cuba', 'Denmark', 'Bangladesh', 'Iran', 'Ireland', 'Liechtenstein', 'Thailand', 'Morocco', 'Panama',
    'Hong Kong', 'Venezuela', 'Ukraine', 'Israel', 'Iceland', 'West Germany', 'East Germany', 'South Korea',
    'Palestine', 'Cyprus', 'Uruguay', 'Mexico', 'Zimbabwe', 'Georgia', 'Montenegro', 'Indonesia', 'Guatemala',
    'Mongolia', 'Azerbaijan', 'Armenia', 'Syria', 'Saudi Arabia', 'Uganda', 'Namibia', 'Switzerland', 'Ethiopia',
    'Latvia', 'Jamaica', 'United Arab Emirates', 'Canada', 'Samoa', 'Czech Republic', 'Mozambique', 'Brazil',
    'Kenya', 'Lebanon', 'Slovenia', 'Dominican Republic', 'Japan', 'Botswana', 'Luxembourg', 'New Zealand',
    'Poland', 'Portugal', 'Australia', 'Cameroon', 'Romania', 'Bulgaria', 'Nepal', 'Austria', 'Egypt',
    'Kazakhstan', 'Serbia', 'South Africa', 'Burkina Faso', 'Bermuda', 'Colombia', 'Hungary', 'Pakistan',
    'Vatican City', 'Mauritius', 'United Kingdom', 'Vietnam', 'Netherlands'
]

# Save to a text file
file_path = "../data/valid_countries.txt"
with open(file_path, "w") as f:
    for country in valid_countries:
        f.write(country + "\n")

# Read the text file into a PySpark DataFrame
df_countries = spark.read.text(file_path).withColumnRenamed("value", "country")

# Show DataFrame
df_countries.show(truncate=False)
