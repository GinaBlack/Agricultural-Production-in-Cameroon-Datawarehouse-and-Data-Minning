import mysql.connector
import matplotlib.pyplot as plt
import pandas as pd
from mysql.connector import Error

# Replace these dummy database credentials with your actual database details
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'uct04bgs101120'
DB_NAME = 'agric_cameroon'

# Optimized queries
queries = {
    "Query 2 + Query 3": """
        SELECT 
            rd.Region_Name,
            td.Year,
            cd.Crop_Name,
            SUM(pf.Quantity) AS Total_Quantity,
            SUM(pf.Area_Harvested) AS Total_Area,
            AVG(pf.Yield) AS Average_Yield
        FROM production_fact pf
        JOIN region_dim rd ON pf.Region_ID = rd.Region_ID
        JOIN crop_dim cd ON pf.Crop_ID = cd.Crop_ID
        JOIN time_dim td ON pf.Time_ID = td.Time_ID
        GROUP BY rd.Region_Name, td.Year, cd.Crop_Name
        ORDER BY rd.Region_Name, td.Year, Total_Quantity DESC;
    """,
    "Query 4 + Query 5": """
        SELECT 
            td.Season,
            cld.Climate_Name,
            cd.Crop_Name,
            SUM(pf.Quantity) AS Total_Quantity,
            AVG(pf.Yield) AS Average_Yield
        FROM production_fact pf
        JOIN time_dim td ON pf.Time_ID = td.Time_ID
        JOIN region_dim rd ON pf.Region_ID = rd.Region_ID
        JOIN climate_dim cld ON rd.Climate_ID = cld.Climate_ID
        JOIN crop_dim cd ON pf.Crop_ID = cd.Crop_ID
        GROUP BY td.Season, cld.Climate_Name, cd.Crop_Name
        ORDER BY td.Season, cld.Climate_Name, Total_Quantity DESC;
    """,
    "Query 6 + Query 8": """
        SELECT 
            sd.Soil_Type,
            rd.Region_Name,
            cd.Crop_Name,
            SUM(pf.Quantity) AS Total_Quantity,
            SUM(pf.Area_Harvested) AS Total_Area,
            AVG(pf.Yield) AS Average_Yield,
            rd.Population AS Region_Population
        FROM production_fact pf
        JOIN region_dim rd ON pf.Region_ID = rd.Region_ID
        JOIN soil_dim sd ON rd.Soil_ID = sd.Soil_ID
        JOIN crop_dim cd ON pf.Crop_ID = cd.Crop_ID
        GROUP BY sd.Soil_Type, rd.Region_Name, cd.Crop_Name, rd.Population
        ORDER BY sd.Soil_Type, Total_Quantity DESC;
    """
}

# Function to execute queries and fetch results
def fetch_query_results(query):
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            headers = [i[0] for i in cursor.description]
            return headers, results
    except Error as e:
        print(f"Error: {e}")
        return None, None
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

# Function to plot a bar chart
def plot_bar_chart(x, y, title, x_label, y_label):
    plt.figure(figsize=(10, 6))
    plt.bar(x, y, color='skyblue')
    plt.title(title, fontsize=14)
    plt.xlabel(x_label, fontsize=12)
    plt.ylabel(y_label, fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=10)
    plt.tight_layout()
    plt.show()

# Function to visualize results
def visualize_results(query_name, headers, results):
    df = pd.DataFrame(results, columns=headers)
    if query_name == "Query 2 + Query 3":
        # Visualize total production by region and year
        grouped = df.groupby(['Region_Name', 'Year'])['Total_Quantity'].sum().reset_index()
        x = grouped.apply(lambda row: f"{row['Region_Name']} ({row['Year']})", axis=1)
        y = grouped['Total_Quantity']
        plot_bar_chart(x, y, "Total Production by Region and Year", "Region (Year)", "Total Quantity")
    elif query_name == "Query 4 + Query 5":
        # Visualize seasonal and climate impact
        grouped = df.groupby(['Season', 'Climate_Name'])['Total_Quantity'].sum().reset_index()
        x = grouped.apply(lambda row: f"{row['Season']} - {row['Climate_Name']}", axis=1)
        y = grouped['Total_Quantity']
        plot_bar_chart(x, y, "Seasonal and Climate Impact on Production", "Season - Climate", "Total Quantity")
    elif query_name == "Query 6 + Query 8":
        # Visualize production summary by soil type and region
        grouped = df.groupby(['Soil_Type', 'Region_Name'])['Total_Quantity'].sum().reset_index()
        x = grouped.apply(lambda row: f"{row['Soil_Type']} - {row['Region_Name']}", axis=1)
        y = grouped['Total_Quantity']
        plot_bar_chart(x, y, "Production by Soil Type and Region", "Soil Type - Region", "Total Quantity")

# Main function
def main():
    for query_name, query in queries.items():
        print(f"Executing {query_name}...")
        headers, results = fetch_query_results(query)
        if headers and results:
            print(f"Visualizing results for {query_name}...")
            visualize_results(query_name, headers, results)

if __name__ == "__main__":
    main()
