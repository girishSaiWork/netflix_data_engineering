# Netflix Data Engineering Project

This project performs data engineering operations on Netflix movies and TV shows dataset from Kaggle using PySpark.

## Project Overview

This data engineering project processes and cleans Netflix content data, performing various transformations and validations using PySpark. The project handles data quality issues, standardizes country names, and enriches the dataset with additional information.

## Dataset

The project uses the following datasets:
- `netflix_titles.csv`: Main dataset containing Netflix movies and TV shows information
- `valid_countries.txt`: Reference data for country name validation
- `countries_languages.json`: Mapping of countries to their languages

Source: [Kaggle Netflix Movies and TV Shows Dataset](https://www.kaggle.com/datasets/anandshaw2001/netflix-movies-and-tv-shows)

## Project Structure
```
├── src/ # Source code directory
│ └── jobs/
│ └── netflix_data_cleaner.py # Main data processing script
│ └── valid_country.py # Country validation utilities
├── tests/ # Test directory
│ └── test_netflix_test.py # Test cases
├── data/ # Data directory
│ └── netflix_titles.csv
│ └── valid_countries.txt
│ └── countries_languages.json
├── .venv/ # Virtual environment
├── requirements.txt # Project dependencies
└── README.md
```

## Features

- Data cleaning and standardization
- Country name validation and standardization
- Date format standardization
- Missing value handling
- Data quality checks
- Language enrichment based on country information

## Requirements

- Python 3.x
- PySpark - In memory data processing framework
- Chispa - PySpark testing package
- Other dependencies listed in requirements.txt

## Setup and Installation

1. Clone the repository
2. Create a virtual environment:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

Run the main data processing script:

```bash
python src/jobs/netflix_data_cleaner.py
```

## Testing

Execute the test suite:
```bash
python -m pytest tests/
```

## License

[Add your license information here]

## Contributors

[Add contributor information here]

