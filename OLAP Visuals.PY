import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt

# Database connection details
def connect_to_database():
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='uct04bgs101120',
            database='agric_cameroon'
        )
        cursor = conn.cursor()
        print("Connected to the database.")
        return conn, cursor
    except mysql.connector.Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None, None

# Check connection and execute query
def execute_query(query):
    conn, cursor = connect_to_database()
    if conn and cursor:
        try:
            cursor.execute(query)
            result = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]  # Extract column names
            df = pd.DataFrame(result, columns=columns)
            return df
        except mysql.connector.Error as e:
            print(f"Error while executing query: {e}")
        finally:
            if conn.is_connected():
                cursor.close()
                conn.close()
                print("MySQL connection is closed.")
    return None

# OLAP Queries
queries = {
    "Total Crop Production by Region and Year": {
        "query": """
            SELECT r.Region_Name, t.Year, SUM(p.Quantity) AS Total_Production
            FROM production_fact p
            JOIN region_dim r ON p.Region_ID = r.Region_ID
            JOIN time_dim t ON p.Time_ID = t.Time_ID
            GROUP BY r.Region_Name, t.Year
            ORDER BY r.Region_Name, t.Year;
        """,
        "purpose": "Aggregates total crop production by region and year."
    },
    "Average Yield per Crop Type by Region": {
        "query": """
            SELECT r.Region_Name, c.Crop_Type, AVG(p.Yield) AS Avg_Yield
            FROM production_fact p
            JOIN crop_dim c ON p.Crop_ID = c.Crop_ID
            JOIN region_dim r ON p.Region_ID = r.Region_ID
            GROUP BY r.Region_Name, c.Crop_Type
            ORDER BY r.Region_Name, c.Crop_Type;
        """,
        "purpose": "Calculates the average yield for each crop type in each region."
    },
    "Total Production by Crop and Climate Type": {
        "query": """
            SELECT c.Climate_Name, c.Crop_Name, SUM(p.Quantity) AS Total_Production
            FROM production_fact p
            JOIN crop_dim c ON p.Crop_ID = c.Crop_ID
            JOIN region_dim r ON p.Region_ID = r.Region_ID
            JOIN climate_dim c ON r.Climate_ID = c.Climate_ID
            GROUP BY c.Climate_Name, c.Crop_Name
            ORDER BY c.Climate_Name, c.Crop_Name;
        """,
        "purpose": "Evaluates total crop production for each crop type under different climate conditions."
    },
    "Year-over-Year Growth in Crop Production by Region": {
        "query": """
            SELECT r.Region_Name, t.Year, 
            SUM(p.Quantity) - LAG(SUM(p.Quantity)) OVER (PARTITION BY r.Region_Name ORDER BY t.Year) AS YoY_Growth
            FROM production_fact p
            JOIN region_dim r ON p.Region_ID = r.Region_ID
            JOIN time_dim t ON p.Time_ID = t.Time_ID
            GROUP BY r.Region_Name, t.Year
            ORDER BY r.Region_Name, t.Year;
        """,
        "purpose": "Calculates year-over-year growth in crop production by region."
    },
    "Top 5 Crops by Market Value and Production Volume": {
        "query": """
            SELECT c.Crop_Name, SUM(p.Quantity) AS Total_Production
            FROM production_fact p
            JOIN crop_dim c ON p.Crop_ID = c.Crop_ID
            GROUP BY c.Crop_Name
            ORDER BY Total_Production DESC
            LIMIT 5;
        """,
        "purpose": "Identifies the top 5 crops based on total production volume."
    },
    "Crop Performance Analysis Across Different Seasons": {
        "query": """
            SELECT t.Season, c.Crop_Name, AVG(p.Yield) AS Avg_Yield
            FROM production_fact p
            JOIN crop_dim c ON p.Crop_ID = c.Crop_ID
            JOIN time_dim t ON p.Time_ID = t.Time_ID
            GROUP BY t.Season, c.Crop_Name
            ORDER BY t.Season, c.Crop_Name;
        """,
        "purpose": "Evaluates the performance of crops during different seasons based on average yield."
    },
    "Soil and Climate Impact on Yield by Region": {
        "query": """
            SELECT r.Region_Name, s.Soil_Type, c.Climate_Name, AVG(p.Yield) AS Avg_Yield
            FROM production_fact p
            JOIN region_dim r ON p.Region_ID = r.Region_ID
            JOIN soil_dim s ON r.Soil_ID = s.Soil_ID
            JOIN climate_dim c ON r.Climate_ID = c.Climate_ID
            GROUP BY r.Region_Name, s.Soil_Type, c.Climate_Name
            ORDER BY r.Region_Name, s.Soil_Type, c.Climate_Name;
        """,
        "purpose": "Examines the impact of soil type and climate conditions on crop yield in different regions."
    },
}

# Execute queries and display results
def run_queries():
    for title, details in queries.items():
        print(f"Query: {title}")
        print(f"Purpose: {details['purpose']}\n")
        df = execute_query(details['query'])
        
        if df is not None and not df.empty:
            # Display the DataFrame
            print(df)

            # Check if there is numeric data for plotting
            numeric_columns = df.select_dtypes(include=['number']).columns

            if not numeric_columns.empty:
                # If numeric data exists, use it for plotting
                y_column = numeric_columns[0]  # Select the first numeric column
                df.plot(kind='bar', x=df.columns[0], y=y_column, legend=False)
                plt.title(title)
                plt.xlabel(df.columns[0])
                plt.ylabel(y_column.replace('_', ' ').title())
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.show()
            else:
                # If no numeric data, count occurrences of the first column for a bar chart
                value_counts = df[df.columns[0]].value_counts()
                value_counts.plot(kind='bar', legend=False)
                plt.title(title)
                plt.xlabel(df.columns[0].replace('_', ' ').title())
                plt.ylabel('Count')
                plt.xticks(rotation=45)
                plt.tight_layout()
                plt.show()
        else:
            print("No data returned from the query.")

# Run all queries
if __name__ == "__main__":
    run_queries()
