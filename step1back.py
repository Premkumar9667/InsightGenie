from flask import Flask  # You imported Flask, but it’s unused — okay to keep or remove

import ollama

# Function to generate response from the trained model
def chat_with_model(user_input):
    try:
        # Define system prompt
        system_prompt = (
            "You are a helpful assistant that converts natural language prompts into SQL queries for a given database schema. "
            "You will:\n"
            "🔹 Understand the user's prompt and identify the intent (e.g., select, filter, aggregate, join).\n"
            "🔹 Use the provided database schema to map the prompt to the correct table and column names.\n"
            "🔹 Generate a syntactically correct SQL query in a simple and clear format.\n\n"
            "⚠️ Rules:\n"
            "- Only generate SQL queries related to the provided schema.\n"
            "- If the prompt is unclear or unrelated to SQL generation, respond: \"Sorry, I can only help with generating SQL queries for the given database schema.\"\n"
            "- If the schema is insufficient or missing, respond: \"Please provide the database schema to generate the SQL query.\"\n\n"
            "The database schema is:\n"
            "Table: Honda_Sales\n"
            "Columns: Sale_ID (VARCHAR), Date (DATE), City (VARCHAR), Region (VARCHAR), Showroom (VARCHAR), Category (VARCHAR), "
            "Product (VARCHAR), Units_Sold (INT), Unit_Price (DECIMAL), Discount_Applied (DECIMAL), Total_Sale (DECIMAL), "
            "Sales_Executive (VARCHAR), Customer_Name (VARCHAR), Phone (VARCHAR), Email (VARCHAR), Payment_Mode (VARCHAR)\n\n"
            "Generate the SQL query based on the user’s prompt."
        )

        # Send the message to the Ollama model
        response = ollama.chat(
            model='llama2:7b-chat',
            messages=[
                {'role': 'system', 'content': system_prompt},
                {'role': 'user', 'content': user_input}
            ]
        )

        # Get the generated response
        return response['message']['content']

    except Exception as e:
        return f"An error occurred: {str(e)}"

# Main chat loop
def main():
    print("Welcome my friend! Type 'quit' to exit.")
    while True:
        user_input = input("> ").strip()
        if user_input.lower() == "quit":
            print("Goodbye!")
            break
        response = chat_with_model(user_input)
        print(f"Assistant: {response}")

if __name__ == "__main__":
    main()
