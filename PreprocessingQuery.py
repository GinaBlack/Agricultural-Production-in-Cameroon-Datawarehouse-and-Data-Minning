import mysql.connector
import csv
from mysql.connector import Error

# Replace these dummy database credentials with your actual database details
DB_HOST = 'localhost'
DB_USER = 'root'
DB_PASSWORD = 'uct04bgs101120'
DB_NAME = 'agric_cameroon'

queries = [
    # Query 1: Basic join for production analysis
    """
    SELECT 
        pf.Production_ID,
        rd.Region_Name,
        cd.Crop_Name,
        td.Year,
        td.Month,
        td.Season,
        pf.Area_Harvested,
        pf.Quantity,
        pf.Yield
    FROM production_fact pf
    JOIN region_dim rd ON pf.Region_ID = rd.Region_ID
    JOIN crop_dim cd ON pf.Crop_ID = cd.Crop_ID
    JOIN time_dim td ON pf.Time_ID = td.Time_ID
    ORDER BY td.Year, td.Month;
    """,
    # Query 2: Total production by region and year
    """
    SELECT 
        rd.Region_Name,
        td.Year,
        SUM(pf.Quantity) AS Total_Quantity,
        AVG(pf.Yield) AS Average_Yield
    FROM production_fact pf
    JOIN region_dim rd ON pf.Region_ID = rd.Region_ID
    JOIN time_dim td ON pf.Time_ID = td.Time_ID
    GROUP BY rd.Region_Name, td.Year
    ORDER BY rd.Region_Name, td.Year;
    """,
    # Query 3: Crop performance by region
    """
    SELECT 
        cd.Crop_Name,
        rd.Region_Name,
        SUM(pf.Quantity) AS Total_Quantity,
        SUM(pf.Area_Harvested) AS Total_Area,
        AVG(pf.Yield) AS Average_Yield
    FROM production_fact pf
    JOIN crop_dim cd ON pf.Crop_ID = cd.Crop_ID
    JOIN region_dim rd ON pf.Region_ID = rd.Region_ID
    GROUP BY cd.Crop_Name, rd.Region_Name
    ORDER BY cd.Crop_Name, Total_Quantity DESC;
    """,
    # Query 4: Seasonal production trends
    """
    SELECT 
        td.Season,
        cd.Crop_Name,
        SUM(pf.Quantity) AS Total_Quantity,
        AVG(pf.Yield) AS Average_Yield
    FROM production_fact pf
    JOIN time_dim td ON pf.Time_ID = td.Time_ID
    JOIN crop_dim cd ON pf.Crop_ID = cd.Crop_ID
    GROUP BY td.Season, cd.Crop_Name
    ORDER BY td.Season, Total_Quantity DESC;
    """,
    # Query 5: Climate impact on production
    """
    SELECT 
        cld.Climate_Name,
        cd.Crop_Name,
        AVG(pf.Yield) AS Average_Yield,
        SUM(pf.Quantity) AS Total_Quantity
    FROM production_fact pf
    JOIN region_dim rd ON pf.Region_ID = rd.Region_ID
    JOIN climate_dim cld ON rd.Climate_ID = cld.Climate_ID
    JOIN crop_dim cd ON pf.Crop_ID = cd.Crop_ID
    GROUP BY cld.Climate_Name, cd.Crop_Name
    ORDER BY cld.Climate_Name, Average_Yield DESC;
    """,
    # Query 6: Production summary by soil type
    """
    SELECT 
        sd.Soil_Type,
        cd.Crop_Name,
        SUM(pf.Quantity) AS Total_Quantity,
        AVG(pf.Yield) AS Average_Yield
    FROM production_fact pf
    JOIN region_dim rd ON pf.Region_ID = rd.Region_ID
    JOIN soil_dim sd ON rd.Soil_ID = sd.Soil_ID
    JOIN crop_dim cd ON pf.Crop_ID = cd.Crop_ID
    GROUP BY sd.Soil_Type, cd.Crop_Name
    ORDER BY sd.Soil_Type, Average_Yield DESC;
    """,
    # Query 7: Yearly crop production comparison
    """
    SELECT 
        cd.Crop_Name,
        td.Year,
        SUM(pf.Quantity) AS Total_Quantity,
        AVG(pf.Yield) AS Average_Yield
    FROM production_fact pf
    JOIN crop_dim cd ON pf.Crop_ID = cd.Crop_ID
    JOIN time_dim td ON pf.Time_ID = td.Time_ID
    GROUP BY cd.Crop_Name, td.Year
    ORDER BY cd.Crop_Name, td.Year;
    """,
    # Query 8: Regional production summary
    """
    SELECT 
        rd.Region_Name,
        SUM(pf.Quantity) AS Total_Quantity,
        SUM(pf.Area_Harvested) AS Total_Area,
        AVG(pf.Yield) AS Average_Yield,
        rd.Population AS Region_Population
    FROM production_fact pf
    JOIN region_dim rd ON pf.Region_ID = rd.Region_ID
    GROUP BY rd.Region_Name, rd.Population
    ORDER BY Total_Quantity DESC;
    """,
    # Query 9: Detailed Production Report
    """
    SELECT 
        pf.Production_ID,
        rd.Region_Name,
        cld.Climate_Name,
        sd.Soil_Type,
        cd.Crop_Name,
        td.Year,
        td.Season,
        pf.Area_Harvested,
        pf.Quantity,
        pf.Yield
    FROM production_fact pf
    JOIN region_dim rd ON pf.Region_ID = rd.Region_ID
    JOIN climate_dim cld ON rd.Climate_ID = cld.Climate_ID
    JOIN soil_dim sd ON rd.Soil_ID = sd.Soil_ID
    JOIN crop_dim cd ON pf.Crop_ID = cd.Crop_ID
    JOIN time_dim td ON pf.Time_ID = td.Time_ID
    ORDER BY td.Year, rd.Region_Name, cd.Crop_Name;
    """
]

def save_to_csv(file_name, headers, data):
    with open(file_name, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
        writer.writerows(data)

def run_queries():
    try:
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )

        if connection.is_connected():
            cursor = connection.cursor()
            for index, query in enumerate(queries, start=1):
                print(f"Executing Query {index}...")
                cursor.execute(query)
                results = cursor.fetchall()
                headers = [i[0] for i in cursor.description]
                
                if index in {1, 9}:  # Special handling for Query 1 and 9
                    print(f"Results for Query {index}: (First 30 rows)")
                    for row in results[:30]:
                        print(row)
                    
                    print(f"\nResults for Query {index}: (Last 30 rows)")
                    for row in results[-30:]:
                        print(row)
                    
                    # Save full results to CSV
                    file_name = f"query_{index}_results.csv"
                    save_to_csv(file_name, headers, results)
                    print(f"Full results for Query {index} saved to {file_name}\n")
                else:
                    print(f"Results for Query {index}:")
                    for row in results:
                        print(row)

                print("\n")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

if __name__ == "__main__":
    run_queries()
