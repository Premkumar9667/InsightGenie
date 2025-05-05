from flask import Flask, request, render_template_string
import mysql.connector
import pandas as pd
import ollama
import re  # For regex-based query correction

app = Flask(__name__)

# Function to generate SQL from natural language prompt
def generate_sql_query(user_input):
    try:
        system_prompt = (
            "You are a helpful assistant that converts natural language prompts into SQL queries for a given database schema. "
            "You will:\n"
            "🔹 Understand the user's prompt and identify the intent (e.g., select, filter, aggregate, join).\n"
            "🔹 Use the provided database schema to map the prompt to the correct table and column names.\n"
            "🔹 Generate a syntactically correct SQL query in a simple and clear format.\n\n"
            "⚠️ Rules:\n"
            "- Only generate SQL queries related to the provided schema.\n"
            "- If the prompt is unclear or unrelated to SQL generation, respond: \"Sorry, I can only help with generating SQL queries for the given database schema.\"\n"
            "- If the schema is insufficient or missing, respond: \"Please provide the database schema to generate the SQL query.\"\n"
            "- For non-null checks, always use `IS NOT NULL` exactly as shown (e.g., `Product IS NOT NULL`). Do NOT use `NOT NULL` or duplicate `IS` (e.g., do NOT write `IS IS NOT NULL`). For example, for 'customers who purchased a product,' include `Product IS NOT NULL` to ensure a product was purchased.\n"
            "- When the prompt asks for 'missing' or 'not provided' data (e.g., 'missing email addresses'), interpret it as records where the column is null or an empty string. Use `IS NULL OR column = ''` for string columns like `Phone` or `Email`. For example, for 'Get sales data with missing email addresses,' the query should be: `SELECT * FROM Honda_Sales WHERE Email IS NULL OR Email = '';`.\n"
            "- Do NOT add conditions for columns that are not mentioned in the prompt. For example, if the prompt only mentions 'missing email addresses,' do NOT add conditions for other columns like `Phone` unless explicitly requested.\n"
            "- When the user asks for a specific number of results (e.g., 'top 5'), include a `LIMIT` clause with the specified number (e.g., `LIMIT 5`).\n"
            "- In the `ORDER BY` clause, ensure the column or alias matches exactly what is defined in the `SELECT` clause (e.g., if `SELECT SUM(Units_Sold) AS Total_Sales`, use `ORDER BY Total_Sales`, not `TotalSales`).\n"
            "- When the user refers to 'total sales' (e.g., in prompts like 'top 5 cities by total sales'), ALWAYS interpret it as the monetary value of sales, which MUST be calculated using the `Total_Sale` column (e.g., `SUM(Total_Sale)`), NOT the number of units sold (`Units_Sold`). For example, for 'top 5 cities by total sales,' the query should be: `SELECT City, SUM(Total_Sale) AS Total_Sales FROM Honda_Sales GROUP BY City ORDER BY Total_Sales DESC LIMIT 5;`.\n"
            "- When the user asks for data from 'last month' or 'previous month,' interpret it as the entire previous month relative to the current date (e.g., if today is May 2025, last month is all of April 2025). Use a date range to cover the whole month, from the first day to the last day of the previous month. For example, for 'Show me all sales from last month,' the query should be: `SELECT * FROM Honda_Sales WHERE Date >= DATE_SUB(DATE_SUB(CURDATE(), INTERVAL 1 MONTH), INTERVAL DAY(CURDATE())-1 DAY) AND Date <= LAST_DAY(DATE_SUB(CURDATE(), INTERVAL 1 MONTH));`. Do NOT use `Date = DATE_SUB(...)` as it only matches a single day.\n"
            "- When the user mentions a specific month and year (e.g., 'March 2025'), calculate the date range for that specific month. For example, for 'Calculate the total revenue generated in March 2025,' use a fixed date range: `SELECT SUM(Total_Sale) AS Total_Revenue FROM Honda_Sales WHERE Date >= '2025-03-01' AND Date <= '2025-03-31';`. Alternatively, calculate the interval dynamically relative to the current date (e.g., if today is May 2025, March 2025 is 2 months back, so use `INTERVAL 2 MONTH`).\n"
            "- When the user mentions a location like 'Chennai' in the context of 'in Chennai' or 'from Chennai,' interpret it as a city and use the `City` column, NOT the `Region` column. The `Region` column should be used for broader areas like 'South India,' while `City` is for specific cities like 'Chennai,' 'Bangalore,' etc. For example, for 'customers who purchased a product in Chennai,' the query should be: `SELECT Customer_Name FROM Honda_Sales WHERE City = 'Chennai' AND Product IS NOT NULL;`.\n"
            "- ONLY return the SQL code inside a code block like ```sql ... ``` with NO explanation.\n\n"

            "The database schema is:\n"
            "Table: Honda_Sales\n"
            "Columns: Sale_ID (VARCHAR), Date (DATE), City (VARCHAR), Region (VARCHAR), Showroom (VARCHAR), Category (VARCHAR), "
            "Product (VARCHAR), Units_Sold (INT), Unit_Price (DECIMAL), Discount_Applied (DECIMAL), Total_Sale (DECIMAL), "
            "Sales_Executive (VARCHAR), Customer_Name (VARCHAR), Phone (VARCHAR), Email (VARCHAR), Payment_Mode (VARCHAR)\n\n"
            "Generate the SQL query based on the user’s prompt."
        )

        response = ollama.chat(
            model='llama2:7b-chat',
            messages=[
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': user_input}
            ]
        )

        return response['message']['content']
    except Exception as e:
        return f"Error generating SQL query: {str(e)}"

# Function to clean the SQL query by removing Markdown code block markers and fixing syntax errors
def clean_sql_query(sql_query):
    # Remove the ```sql and ``` markers, and strip any whitespace
    if sql_query.startswith('```sql'):
        sql_query = sql_query.replace('```sql', '').replace('```', '').strip()
    
    # Fallback: Replace incorrect `NOT NULL` with `IS NOT NULL`
    sql_query = re.sub(r'(\w+)\s+NOT\s+NULL', r'\1 IS NOT NULL', sql_query, flags=re.IGNORECASE)
    
    # Fallback: Fix incorrect `IS IS NOT NULL` by removing the extra `IS`
    sql_query = re.sub(r'(\w+)\s+IS\s+IS\s+NOT\s+NULL', r'\1 IS NOT NULL', sql_query, flags=re.IGNORECASE)
    
    # Fallback: Fix incorrect `Region = 'Chennai'` with `City = 'Chennai'`
    if "Region = 'Chennai'" in sql_query:
        sql_query = sql_query.replace("Region = 'Chennai'", "City = 'Chennai'")
    
    # Fallback: Fix incorrect `Phone IS NOT NULL` with `Phone IS NULL` for "missing" data
    if "missing phone" in request.form.get('prompt', '').lower() and "Phone IS NOT NULL" in sql_query:
        sql_query = sql_query.replace("Phone IS NOT NULL", "Phone IS NULL")
    
    # Enhancement: Add check for empty strings for "missing" data in string columns like Phone or Email
    prompt_lower = request.form.get('prompt', '').lower()
    if "missing phone" in prompt_lower and "Phone IS NULL" in sql_query and "Phone = ''" not in sql_query:
        sql_query = sql_query.replace("Phone IS NULL", "Phone IS NULL OR Phone = ''")
    elif "missing email" in prompt_lower and "Email IS NULL" in sql_query and "Email = ''" not in sql_query:
        sql_query = sql_query.replace("Email IS NULL", "Email IS NULL OR Email = ''")
    elif "missing email" in prompt_lower and "Email = ''" in sql_query and "Email IS NULL" not in sql_query:
        sql_query = sql_query.replace("Email = ''", "Email IS NULL OR Email = ''")
    
    # Fallback: Remove unnecessary `Phone IS NOT NULL` condition if not mentioned in the prompt
    if "Phone IS NOT NULL" in sql_query and "phone" not in prompt_lower:
        # Remove the condition and any associated AND/OR
        sql_query = re.sub(r'\s*AND\s*Phone\s+IS\s+NOT\s+NULL', '', sql_query, flags=re.IGNORECASE)
        sql_query = re.sub(r'Phone\s+IS\s+NOT\s+NULL\s*AND\s*', '', sql_query, flags=re.IGNORECASE)
        # If it's the only condition, remove it entirely
        sql_query = re.sub(r'WHERE\s*Phone\s+IS\s+NOT\s+NULL', '', sql_query, flags=re.IGNORECASE)
    
    # Fallback: Fix alias mismatch in ORDER BY (e.g., TotalSales -> Total_Sales)
    if ' AS ' in sql_query and 'ORDER BY' in sql_query:
        select_part = sql_query.split('ORDER BY')[0]
        order_by_part = sql_query.split('ORDER BY')[1]
        # Find aliases in SELECT clause
        aliases = re.findall(r' AS (\w+)', select_part, re.IGNORECASE)
        for alias in aliases:
            # Replace incorrect alias in ORDER BY (e.g., TotalSales with Total_Sales)
            incorrect_alias = alias.replace('_', '')
            if incorrect_alias in order_by_part:
                sql_query = sql_query.replace(incorrect_alias, alias)
    
    # Fallback: If the query uses SUM(Units_Sold) AS Total_Sales, replace with SUM(Total_Sale)
    if 'SUM(Units_Sold) AS Total_Sales' in sql_query:
        sql_query = sql_query.replace('SUM(Units_Sold) AS Total_Sales', 'SUM(Total_Sale) AS Total_Sales')
    
    # Fallback: Fix incorrect `Date = DATE_SUB` for "last month" queries, but only if it's not already a range
    if 'Date = DATE_SUB' in sql_query and 'Date >=' not in sql_query and 'Date <=' not in sql_query:
        # Replace with a range covering the entire previous month
        date_sub_part = re.search(r'DATE_SUB\([^,]+,\s*INTERVAL\s*(\d+)\s*MONTH\)', sql_query)
        if date_sub_part:
            interval = date_sub_part.group(1)  # e.g., 1 for INTERVAL 1 MONTH
            date_sub_expr = f"DATE_SUB(CURDATE(), INTERVAL {interval} MONTH)"
            # Construct the correct range
            start_of_month = f"DATE_SUB({date_sub_expr}, INTERVAL DAY(CURDATE())-1 DAY)"
            end_of_month = f"LAST_DAY({date_sub_expr})"
            # Replace the incorrect `Date = ...` with the correct range
            sql_query = sql_query.replace(f"Date = {date_sub_part.group(0)}", f"Date >= {start_of_month} AND Date <= {end_of_month}")
    
    return sql_query

# Function to run SQL query on MySQL DB
def retrieve_data_from_db(sql_query):
    try:
        conn = mysql.connector.connect(
            host='localhost',
            user='root',
            password='',
            database='honda'
        )
        df = pd.read_sql(sql_query, conn)
        conn.close()
        return df
    except Exception as e:
        return f"Error executing query: {e}"

# Main Flask route
@app.route('/', methods=['GET', 'POST'])
def home():
    result_html = ''
    sql_query = ''
    if request.method == 'POST':
        prompt = request.form['prompt']

        # Step 1: Get SQL query (with Markdown formatting)
        sql_query = generate_sql_query(prompt)

        # Check if the assistant refused or asked for schema
        if sql_query.startswith("Sorry") or sql_query.startswith("Please"):
            result_html = f"<p style='color:red;'>{sql_query}</p>"
        else:
            # Step 2: Clean the SQL query for database execution
            clean_query = clean_sql_query(sql_query)

            # Step 3: Run the cleaned SQL query on DB
            result = retrieve_data_from_db(clean_query)

            # Step 4: Show data in table
            if isinstance(result, str) and result.startswith('Error'):
                result_html = f"<p style='color:red;'>{result}</p>"
            elif isinstance(result, pd.DataFrame):
                if result.empty:
                    result_html = '<p>No data returned.</p>'
                else:
                    result_html = result.to_html(classes='table table-striped', index=False)
            else:
                result_html = '<p>Unexpected error.</p>'

    return render_template_string('''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Honda Sales Query</title>
        <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.0.0/dist/css/bootstrap.min.css">
    </head>
    <body class="p-4">
        <h1>Honda Sales Query (Natural Language → SQL → Database)</h1>
        <form method="POST" class="form-inline mb-3">
            <input type="text" name="prompt" class="form-control mr-2" placeholder="Enter your question" style="width: 400px;" required>
            <button type="submit" class="btn btn-primary">Submit</button>
        </form>
        <h4>Generated SQL Query:</h4>
        <pre>{{ sql_query }}</pre>
        <h4>Result:</h4>
        {{ result_html|safe }}
    </body>
    </html>
    ''', sql_query=sql_query, result_html=result_html)

if __name__ == '__main__':
    app.run(debug=True)