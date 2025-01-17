import pandas as pd
from sqlalchemy import create_engine
import logging
from scipy.stats import zscore

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection details
DATABASE_CONFIG = {
    "username": "root",
    "password": "uct04bgs101120",
    "host": "localhost",
    "database": "agric_cameroon"
}

# Queries to retrieve data from tables
QUERIES = {
    "production_fact": "SELECT * FROM production_fact",
    "region_dim": "SELECT Region_ID, Region_Name, Climate_ID, Soil_ID FROM region_dim",
    "crop_dim": "SELECT Crop_ID, Crop_Name, Growth_Duration FROM crop_dim",
    "time_dim": "SELECT Time_ID, Season FROM time_dim",
    "climate_dim": "SELECT Climate_ID, Climate_Name, Avg_Temperature FROM climate_dim",
    "soil_dim": "SELECT Soil_ID, Soil_Type FROM soil_dim"
}

def main():
    # Create database connection using SQLAlchemy
    engine = create_engine(
        f"mysql+mysqlconnector://{DATABASE_CONFIG['username']}:{DATABASE_CONFIG['password']}@{DATABASE_CONFIG['host']}/{DATABASE_CONFIG['database']}"
    )
    logging.info("Connected to the database.")

    try:
        # Read data from database
        production_fact_df = pd.read_sql(QUERIES["production_fact"], engine)
        region_dim_df = pd.read_sql(QUERIES["region_dim"], engine)
        crop_dim_df = pd.read_sql(QUERIES["crop_dim"], engine)
        time_dim_df = pd.read_sql(QUERIES["time_dim"], engine)
        climate_dim_df = pd.read_sql(QUERIES["climate_dim"], engine)
        soil_dim_df = pd.read_sql(QUERIES["soil_dim"], engine)

        # Check for missing data
        logging.info("Checking for missing data:")
        logging.info(production_fact_df.isnull().sum())

        # Fill missing values using forward fill method
        production_fact_df = production_fact_df.ffill()
        logging.info("Missing values filled using forward fill method.")

        # Perform joins to get the final dataframe
        merged_df = production_fact_df.merge(region_dim_df, on="Region_ID", how="inner")
        merged_df = merged_df.merge(crop_dim_df, on="Crop_ID", how="inner")
        merged_df = merged_df.merge(time_dim_df, on="Time_ID", how="inner")
        merged_df = merged_df.merge(climate_dim_df, on="Climate_ID", how="inner")
        merged_df = merged_df.merge(soil_dim_df, on="Soil_ID", how="inner")

        # Select specific columns to display
        final_columns = [
            "Production_ID", "Region_Name", "Crop_Name", "Time_ID",
            "Soil_Type", "Climate_Name", "Avg_Temperature",
            "Growth_Duration", "Season", "Quantity"
        ]
        result_df = merged_df[final_columns].copy()

        # Remove duplicate rows
        result_df = result_df.drop_duplicates()
        logging.info("Duplicate rows have been removed.")

        # Normalize 'Quantity' column and remove outliers using Z-score
        result_df['Quantity_zscore'] = zscore(result_df['Quantity'])
        result_df = result_df[result_df['Quantity_zscore'].abs() <= 3].copy()
        result_df.drop(columns=['Quantity_zscore'], inplace=True)
        logging.info("Outliers removed based on Z-score.")

        # Display the result in the terminal
        logging.info("Displaying the final data:")
        print(result_df)

        # Save the result to a CSV file
        output_file = "joins_production.csv"
        result_df.to_csv(output_file, index=False)
        logging.info(f"Data has been successfully written to {output_file}.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")
    finally:
        engine.dispose()
        logging.info("Database connection closed.")

if __name__ == "__main__":
    main()
