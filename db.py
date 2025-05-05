import mysql.connector
import pandas as pd
import uuid

# Step 1: Read the Excel file
data = pd.read_excel('honda_sales_data.xlsx')

# Step 2: Convert pandas Timestamp dates to SQL-compatible DATE format (YYYY-MM-DD)
def timestamp_to_mysql_date(date_value):
    if pd.isna(date_value):
        return None
    try:
        # Check if the value is a pandas Timestamp
        if isinstance(date_value, pd.Timestamp):
            return date_value.strftime('%Y-%m-%d')
        # If it's already a string in YYYY-MM-DD format, return as-is
        elif isinstance(date_value, str) and len(date_value) >= 10:
            # Validate the string format (basic check for YYYY-MM-DD)
            try:
                pd.to_datetime(date_value, format='%Y-%m-%d')
                return date_value[:10]  # Take only the YYYY-MM-DD part
            except ValueError:
                print(f"Invalid date string format: {date_value}")
                return None
        else:
            print(f"Unexpected date format: {date_value}")
            return None
    except Exception as e:
        print(f"Error converting date {date_value}: {e}")
        return None

# Apply the conversion to the Date column
data['Date'] = data['Date'].apply(timestamp_to_mysql_date)

# Step 3: Ensure Sale_ID is a UUID (already provided in the Excel file, but verify)
data['Sale_ID'] = data['Sale_ID'].astype(str)

# Step 4: Display basic info about the data for verification
print("First 5 rows of the data after date conversion:")
print(data.head())
print("\nData shape:", data.shape)
print("\nData info:")
print(data.info())
print("\nMissing values:")
print(data.isnull().sum())
print("\nData description:")
print(data.describe())

# Step 5: Connect to MySQL database
conn = mysql.connector.connect(
    host="localhost",
    user="root",       # Change to your MySQL username
    password="",   # Change to your MySQL password
    database="honda"    # Change to your MySQL database name
)
cursor = conn.cursor()

# Step 6: Drop the table if it exists (optional, for testing purposes)
cursor.execute("DROP TABLE IF EXISTS Honda_Sales")

# Step 7: Create the Honda_Sales table with appropriate schema
cursor.execute("""
    CREATE TABLE IF NOT EXISTS Honda_Sales (
        Sale_ID VARCHAR(36) PRIMARY KEY,
        Date DATE,
        City VARCHAR(50),
        Region VARCHAR(50),
        Showroom VARCHAR(50),
        Category VARCHAR(20),
        Product VARCHAR(50),
        Units_Sold INT,
        Unit_Price DECIMAL(10, 2),
        Discount_Applied DECIMAL(10, 2),
        Total_Sale DECIMAL(12, 2),
        Sales_Executive VARCHAR(100),
        Customer_Name VARCHAR(100),
        Phone VARCHAR(20),
        Email VARCHAR(100),
        Payment_Mode VARCHAR(20),
        CONSTRAINT chk_units_sold CHECK (Units_Sold >= 0),
        CONSTRAINT chk_unit_price CHECK (Unit_Price >= 0),
        CONSTRAINT chk_discount_applied CHECK (Discount_Applied >= 0),
        CONSTRAINT chk_total_sale CHECK (Total_Sale >= 0)
    )
""")

# Step 8: Create indexes for performance
cursor.execute("CREATE INDEX IF NOT EXISTS idx_date ON Honda_Sales (Date)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_city ON Honda_Sales (City)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_region ON Honda_Sales (Region)")
cursor.execute("CREATE INDEX IF NOT EXISTS idx_product ON Honda_Sales (Product)")

# Commit the table creation
conn.commit()

# Step 9: Insert data into the Honda_Sales table
for _, row in data.iterrows():
    # Handle potential NaN values
    row = row.fillna({
        'Sale_ID': str(uuid.uuid4()),
        'Date': None,
        'City': '',
        'Region': '',
        'Showroom': '',
        'Category': '',
        'Product': '',
        'Units_Sold': 0,
        'Unit_Price': 0.0,
        'Discount_Applied': 0.0,
        'Total_Sale': 0.0,
        'Sales_Executive': '',
        'Customer_Name': '',
        'Phone': '',
        'Email': '',
        'Payment_Mode': ''
    })
    
    cursor.execute("""
        INSERT INTO Honda_Sales (
            Sale_ID, Date, City, Region, Showroom, Category, Product, 
            Units_Sold, Unit_Price, Discount_Applied, Total_Sale, 
            Sales_Executive, Customer_Name, Phone, Email, Payment_Mode
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        row['Sale_ID'],
        row['Date'],
        row['City'],
        row['Region'],
        row['Showroom'],
        row['Category'],
        row['Product'],
        int(row['Units_Sold']) if pd.notna(row['Units_Sold']) else 0,
        float(row['Unit_Price']) if pd.notna(row['Unit_Price']) else 0.0,
        float(row['Discount_Applied']) if pd.notna(row['Discount_Applied']) else 0.0,
        float(row['Total_Sale']) if pd.notna(row['Total_Sale']) else 0.0,
        row['Sales_Executive'],
        row['Customer_Name'],
        row['Phone'],
        row['Email'],
        row['Payment_Mode']
    ))

# Step 10: Commit the data insertion
conn.commit()

# Step 11: Fetch and display the data from the database for verification
cursor.execute("SELECT * FROM Honda_Sales")
columns = [col[0] for col in cursor.description]
data_fetched = cursor.fetchall()
df = pd.DataFrame(data_fetched, columns=columns)

print("\nFirst 5 rows from the database:")
print(df.head())

# Step 12: Close the cursor and connection
cursor.close()
conn.close()