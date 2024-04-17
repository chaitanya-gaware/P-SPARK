import csv
import random

# Function to generate random product names
def generate_product_name():
    products = ["Shirt", "Pants", "Shoes", "Hat", "Jacket", "Dress", "Socks", "Skirt", "Jeans", "Sweater"]
    return random.choice(products)

# Function to generate random price
def generate_price():
    return round(random.uniform(10, 1000), 2)

# Function to generate total value based on price and quantity
def generate_total_value(price, quantity):
    return round(price * quantity, 2)

# Generate CSV file
with open('sales_transactions.csv', 'w', newline='') as csvfile:
    fieldnames = ['ID', 'Product Name', 'Price', 'Total Value']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    for i in range(1, 1001):
        product_name = generate_product_name()
        price = generate_price()
        quantity = random.randint(1, 10)
        total_value = generate_total_value(price, quantity)
        writer.writerow({'ID': i, 'Product Name': product_name, 'Price': price, 'Total Value': total_value})

print("CSV file generated successfully.")
