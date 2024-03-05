import time
import csv
from bs4 import BeautifulSoup
from selenium.common import ElementNotInteractableException
from selenium.webdriver import Edge
from selenium.webdriver.common.by import By

# Replace YOUR-PATH-TO-CHROMEDRIVER with your chromedriver location
driver = Edge()

driver.get('https://flightradar24.com/data/airports/clt/arrivals')

time.sleep(3)

# Wait for the button to appear
button = driver.find_element(By.ID, 'onetrust-accept-btn-handler')

# Click on the button
button.click()

time.sleep(3)

# Function to click on the "Load earlier flights" button and wait until it disappears
def click_load_earlier_until_disappear():
    while True:
        try:
            button = driver.find_element(By.XPATH,'//button[contains(@class, "btn-flights-load") and contains(text(), "Load earlier flights")]')
            button.click()
            time.sleep(2)  # Adjust the sleep time according to your needs
        except ElementNotInteractableException:
            break

# Call the function to repeat the process
click_load_earlier_until_disappear()

# Get the HTML content of the page
html = driver.page_source

# Parse the HTML content
soup = BeautifulSoup(html, 'html.parser')

# Find the table element
table = soup.find('table', class_='table table-condensed table-hover data-table m-n-t-15')

# Extract data from the table
data = []
if table:
    rows = table.find_all('tr')
    for row in rows:
        cells = row.find_all('td')
        if cells:
            # Ensure consistent structure of each row
            row_data = [cell.text.strip() for cell in cells]
            # If row has fewer fields, add empty strings to match the expected number of fields
            if len(row_data) < 7:
                row_data.extend([''] * (7 - len(row_data)))
            data.append(row_data)

# Split the data into halves
split_index = len(data) // 2
second_half_data = data[split_index-1:]

# Write the extracted data to a CSV file
csv_file = 'output.csv'
with open(csv_file, 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerows(second_half_data)

# Print a message indicating the CSV file has been created
print("CSV file created successfully.")

