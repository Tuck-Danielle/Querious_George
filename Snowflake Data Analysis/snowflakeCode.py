# Snowpark for Python
from snowflake.snowpark import Session
from snowflake.snowpark.version import VERSION
import snowflake.snowpark.functions as F
from snowflake.snowpark.types import DecimalType
from snowflake.snowpark.functions import col

# Data Science Libs
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

#UI
import tkinter as tk
from tkinter import messagebox
from datetime import datetime, timedelta

# Misc
import json
import joblib

# Load Snowflake connection parameters from JSON file
connection_parameters = json.load(open('connection.json'))

# Create a Snowpark session
session = Session.builder.configs(connection_parameters).create()
session.sql_simplifier_enabled = True

# Execute SQL query using Snowpark session
DEMO_TABLE = 'PURCHASE_ORDER_HISTORY'
tableName = f"{session.get_current_database()}.{session.get_current_schema()}.{DEMO_TABLE}"
orders_df = session.table(tableName)

# Convert Snowpark DataFrame to Pandas DataFrame
data = orders_df.collect()
df = pd.DataFrame(data)

# Perform operations on Pandas DataFrame
median_targets = df.groupby(["VENDOR_ID"])["TARGET"].median()

#Print the median targets
#print(median_targets)


# Plotting
median_targets.plot(kind='bar', figsize=(10, 6))
plt.title('Median Target by Vendor')
plt.xlabel('Vendor ID')
plt.ylabel('Median Target')
plt.xticks(rotation=90, fontsize=10, ha='right')  # Adjust rotation and fontsize
plt.grid(axis='y')

# Show the plot
plt.tight_layout()

plt.savefig('median_target_plot.png')

plt.show()


def calculate_arrival():
    # Get the vendor ID entered by the user
    vendor_id = vendor_entry.get()
    # Get the estimated date
    date_str = date_entry.get()

    try:
        # Filter the DataFrame for the specified vendor ID
        filtered_df = df[df['VENDOR_ID'] == vendor_id]

        # Calculate the median target for the specified vendor ID
        median_target = filtered_df['TARGET'].median()

        # Parse the estimated date string into a datetime object
        current_date = datetime.strptime(date_str, "%Y-%m-%d").date()
        
        # Calculate the estimated arrival date
        est_date = current_date + timedelta(days=int(median_target))

        # Display the estimated arrival date in a message box
        messagebox.showinfo("Calculate Arrival", f"Based on previous delivery data with {vendor_id} our estimated delivery date is: {est_date}")

    except Exception as e:
        # If the information entered by the user is not valid, display an error message
         messagebox.showerror("Error", "Must be a valid information")



# Close the Snowpark session
session.close()

# Create a simple UI
root = tk.Tk()
root.title("Arrival Calculator based on Vender ID")

#Size of the UI
root.geometry("300x150")

# Label and Entry for entering the vendor ID
vendor_label = tk.Label(root, text="Enter Vendor ID:")
vendor_label.pack()
vendor_entry = tk.Entry(root)
vendor_entry.pack()

# Label and Entry for entering the estimated date
date_label = tk.Label(root, text="Enter Vendor Estimated Arrival Date: (YYYY-MM-DD)")
date_label.pack()
date_entry = tk.Entry(root)
date_entry.pack()



# Button to calculate the median target
calculate_button = tk.Button(root, text="Calculate Arrival", command=calculate_arrival)
calculate_button.pack()

# Run the Tkinter event loop
root.mainloop()


