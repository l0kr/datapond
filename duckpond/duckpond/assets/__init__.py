from dagster import asset 
from datapond import SQL 
import pandas as pd

@asset 
def population() -> SQL:
# Use pandas to scrape wikipedia data and store it in DataFrame,
# queries it via DuckDB and storea the results as a new Parquet file on S3
    df = pd.read_html(
         "https://en.wikipedia.org/wiki/List_of_countries_by_population_(United_Nations)"

    )[0]

    df.columns = [
        "country",
        "continent",
        "subregion",
        "population_2018",
        "population_2019",
        "pop_change",
    ]
    df["pop_change"] = [
        float(str(row).rstrip("%").replace("\u2212", "-")) for row in df["pop_change"]
    ]
    return SQL("select * from $df", df=df)

@asset
def continent_population(population: SQL) -> SQL:
    # reads the stored data with duckdb and creates a new transformed parquet file
    # and stores it on S3
    return SQL(
        "select continent, avg(pop_change) as avg_pop_change from $population group by 1 order by 2 desc",
        population=population,
    )