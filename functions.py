from bs4 import BeautifulSoup
import pandas as pd
import json
import aiofiles
import asyncio
import statistics
async def Dataframe1(file_path):
    # Read the HTML content
    with open(file_path, 'r') as file:
        content = file.read()

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')

    # Find all the table rows in the HTML content
    rows = soup.find_all('tr')

    # Initialize an empty list to store the extracted data
    extracted_data = []

    # Iterate through each row and extract columns of interest (Particulars and Debit)
    for row in rows:
        # Extract all the columns (td) in the row
        cols = row.find_all('td')

        # Check if the row contains data of interest based on the expected number of columns
        if len(cols) > 11:
            date = cols[1].get_text(strip=True)
            particulars = cols[4].get_text(strip=True)
            vchtype = cols[7].get_text(strip=True)
            debit = cols[10].get_text(strip=True)

            # Add the extracted data to the list if it's not empty or a space character
            if date and not date.isspace() and particulars and not particulars.isspace() and vchtype and not vchtype.isspace() and debit and not debit.isspace():
                extracted_data.append([date, particulars, vchtype, debit])

    # Create a pandas DataFrame from the extracted data
    particulars_debit_df = pd.DataFrame(extracted_data, columns=['Date', 'Particulars', 'vchtype', 'debit'])
    dataframe = particulars_debit_df.iloc[1:]

    return dataframe

async def Dataframe2(file_path):
    # Read the HTML content asynchronously
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()

    # Create a DataFrame from the HTML content
    soup = BeautifulSoup(content, 'html.parser')
    rows = soup.find_all('tr')

    # Extract data into a list
    extracted_data = []
    for row in rows:
        cols = row.find_all('td')
        if len(cols) > 1:
            particulars = cols[1].get_text(strip=True)
            debit = cols[5].get_text(strip=True)
            if particulars and not particulars.isspace() and debit and not debit.isspace():
                extracted_data.append([particulars, debit])

    # Create a pandas DataFrame from the extracted data
    particulars_debit_df = pd.DataFrame(extracted_data, columns=['Particulars', 'Debit'])
    dataframe_directexpenses = particulars_debit_df.iloc[1:]

    # Clean and convert the 'Debit' column to numeric
    df = pd.DataFrame(dataframe_directexpenses['Debit'])
    df['Debit'] = pd.to_numeric(df['Debit'].str.replace(',', ''), errors='coerce')
    df['Debit'].fillna(0, inplace=True)
    df['Debit'] = df['Debit'].astype(float)

    return df

async def DataFrame(file_path):
    # Create a pandas DataFrame from the CSV content
    df = pd.read_csv(file_path)

    return df

async def extract_net_profit(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')

      # Find all the table rows in the HTML content
    rows = soup.find_all('tr')

    # Initialize an empty list to store the extracted data
    extracted_data = []

    # Iterate through each row and extract columns of interest (Particulars and Debit)
    for row in rows:
        # Extract all the columns (td) in the row
        cols = row.find_all('td')

        # Check if the row contains data of interest based on the expected number of columns
        if len(cols) > 10:
            particulars = cols[2].get_text(strip=True)
            amount = cols[9].get_text(strip=True)
            extracted_data.append([particulars, amount])

    # Create a pandas DataFrame from the extracted data
    particulars_debit_df = pd.DataFrame(extracted_data, columns=['Particulars', 'amount'])

    # Extract the net profit from the DataFrame
    net_profit = particulars_debit_df['amount'][9]

    return net_profit

async def extract_depreciation(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')

    # Find all the table rows in the HTML content
    rows = soup.find_all('tr')

    # Initialize an empty list to store the extracted data
    extracted_data = []

    # Iterate through each row and extract columns of interest (Particulars and Credit)
    for row in rows:
        # Extract all the columns (td) in the row
        cols = row.find_all('td')

        # Check if the row contains data of interest based on the expected number of columns
        if len(cols) > 10:
            particulars = cols[2].get_text(strip=True)
            credit = cols[9].get_text(strip=True)
            extracted_data.append([particulars, credit])

    # Create a pandas DataFrame from the extracted data
    particulars_credit_df = pd.DataFrame(extracted_data, columns=['Particulars', 'credit'])

    # Extract the depreciation from the DataFrame
    depreciation = particulars_credit_df['credit'][12]

    return depreciation

async def extract_interest_and_financial(file_path):
    with open(file_path, 'r') as file:
        content = file.read()

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')

    # Find all the table rows in the HTML content
    rows = soup.find_all('tr')

    # Initialize an empty list to store the extracted data
    extracted_data = []

    # Iterate through each row and extract columns of interest (Particulars and Debit)
    for row in rows:
        # Extract all the columns (td) in the row
        cols = row.find_all('td')

        # Check if the row contains data of interest based on expected number of columns
        if len(cols) > 10:
            particulars = cols[2].get_text(strip=True)
            debit = cols[6].get_text(strip=True)
            if particulars and not particulars.isspace():
                extracted_data.append([particulars, debit])

    # Create a pandas DataFrame from the extracted data
    particulars_debit_df = pd.DataFrame(extracted_data, columns=['Particulars', 'Debit'])

    # Extract the Interest and Financial value from the DataFrame
    interest_and_financial = particulars_debit_df['Debit'][9]

    return interest_and_financial

async def extract_Gross_Sales_data(file_path):
    async with aiofiles.open(file_path, 'r') as file:
        html_content = await file.read()

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')

    # Find all the table rows in the HTML content
    rows = soup.find_all('tr')

    # Initialize an empty list to store the extracted data
    extracted_data = []

    # Iterate through each row and extract columns of interest (Particulars, Debit, and Credit)
    for row in rows:
        # Extract all the columns (td) in the row
        cols = row.find_all('td')

        # Check if the row contains data of interest based on the expected number of columns
        if len(cols) > 10:
            particulars = cols[2].get_text(strip=True)
            debit = cols[6].get_text(strip=True)
            credit = cols[9].get_text(strip=True)
            
            # Add the extracted data to the list if particulars is not empty or a space character
            if particulars and not particulars.isspace():
                extracted_data.append([particulars, debit, credit])

    # Create a pandas DataFrame from the extracted data
    particulars_debit_df = pd.DataFrame(extracted_data, columns=['Particulars', 'debit', 'credit'])

    return particulars_debit_df

async def extract_discount(file_path):
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')

    # Find all the table rows in the HTML content
    rows = soup.find_all('tr')

    # Initialize an empty list to store the extracted data
    extracted_data = []

    # Iterate through each row and extract columns of interest (Particulars and Debit)
    for row in rows:
        # Extract all the columns (td) in the row
        cols = row.find_all('td')

        # Check if the row contains data of interest based on expected number of columns
        if len(cols) > 10:
            particulars = cols[2].get_text(strip=True)
            debit = cols[6].get_text(strip=True)
            
            # Add the extracted data to the list if particulars is not empty or a space character
            if particulars and not particulars.isspace():
                extracted_data.append([particulars, debit])

    # Create a pandas DataFrame from the extracted data
    particulars_debit_df = pd.DataFrame(extracted_data, columns=['Particulars', 'debit'])

    # Extract Grandtotal and convert it to a float
    grandtotal = particulars_debit_df['debit'][1]
    grandtotal_float_discount = float(grandtotal.replace(',', ''))

    return grandtotal_float_discount

async def extract_discount_disallowed(file_path):
    async with aiofiles.open(file_path, 'r') as file:
        content = await file.read()

    # Parse the HTML content with BeautifulSoup
    soup = BeautifulSoup(content, 'html.parser')

    # Find all the table rows in the HTML content
    rows = soup.find_all('tr')

    # Initialize an empty list to store the extracted data
    extracted_data = []

    # Iterate through each row and extract columns of interest (Particulars and Debit)
    for row in rows:
        # Extract all the columns (td) in the row
        cols = row.find_all('td')

        # Check if the row contains data of interest based on expected number of columns
        if len(cols) > 10:
            particulars = cols[2].get_text(strip=True)
            debit = cols[6].get_text(strip=True)
            
            # Add the extracted data to the list if particulars is not empty or a space character
            if particulars and not particulars.isspace():
                extracted_data.append([particulars, debit])

    # Create a pandas DataFrame from the extracted data
    particulars_debit_df = pd.DataFrame(extracted_data, columns=['Particulars', 'Debit'])

    # Extract Grandtotal and convert it to a float
    grandtotal = particulars_debit_df['Debit'][33]
    grandtotal_float_discount_disallowed = float(grandtotal.replace(',', ''))

    return grandtotal_float_discount_disallowed

async def Card1(dataframe):
    total_footfall = int(dataframe.shape[0] / 2)

    # Create a JSON with the total footfall value
    result_json = {'Total_Footfall': total_footfall}

    return json.dumps(result_json)


async def Card2(dataframe):
     # Extract and clean the 'debit' column
    df = pd.DataFrame(dataframe['debit'])
    df['debit'] = pd.to_numeric(df['debit'].str.replace(',', ''), errors='coerce')
    df['debit'].fillna(0, inplace=True)

    # Calculate revenue
    totalsum = df['debit']
    revenue = (totalsum.sum()) / 2

    # Create a JSON with the revenue value
    result_json = {'Revenue': revenue}

    return json.dumps(result_json)

async def Card3(df):
    # Calculate total revenue
    total_revenue = (df['Debit'].sum()) / 2

    # Create a JSON with the total revenue value
    result_json = {'Total_Revenue': total_revenue}

    return json.dumps(result_json)

async def Card4(netprofit_file_path,Depreciaiton_file_path,interestnfinacial_file_path):
    netprofit= await extract_net_profit(netprofit_file_path)
    depreciation= await extract_depreciation(Depreciaiton_file_path)
    InterestandFinacial=await extract_interest_and_financial(interestnfinacial_file_path)

    depreciation_float = float(depreciation.replace(',', ''))
    netprofit_float = float(netprofit.replace(',',''))
    InterestandFinacial_float=float(InterestandFinacial.replace(',',''))

    EBIDTA = depreciation_float + netprofit_float +InterestandFinacial_float
    
    result_json = {'EBIDTA': EBIDTA}

    return json.dumps(result_json)


async def Card5(df):
    # Replace 'your_column' with the actual name of the column you're interested in
    column_name = 'Particulars'

    # Use the value_counts() function to get the count of each unique value in the specified column
    count = ((df[column_name].value_counts())/2)

    # Print the results 
    # print(count)
    total12 = int(count.shape[0]/2)
    result_json = {"Count of patients revisited: ": total12}

    return json.dumps(result_json)

async def Card6(df):
    column_data = df['debit'].tolist()
    median_value = statistics.median(column_data)

    # Create a JSON with the median value
    result_json = {'Median_Value': median_value}

    return json.dumps(result_json)

async def Card7(dataframe2):
    # Clean and convert the 'Debit' column to numeric
    df = pd.DataFrame(dataframe2['Debit'])
    df['Debit'] = pd.to_numeric(df['Debit'].replace(',', ''), errors='coerce')
    df['Debit'].fillna(0, inplace=True)

    # Calculate total revenue
    totalrev = (df['Debit'].sum()) / 2

    # card7 = revenue/268
    # print(card7)
    grandtotal = totalrev/268
    # Create a JSON with the grand_total value
    result_json = {'Grand_Total': grandtotal}

    return json.dumps(result_json)

async def Card8(dataframe1,dataframe2):
    df = pd.DataFrame(dataframe1['debit'])
    df['debit'] = pd.to_numeric(df['debit'].str.replace(',', ''), errors='coerce')
    df['debit'].fillna(0, inplace=True)

    # Calculate revenue
    totalsum = df['debit']
    total = (totalsum.sum()) / 2

    totalrev = (dataframe2['Debit'].sum()) / 2
    
    card8=total-totalrev

    result_json = {'Card8': card8}
    return json.dumps(result_json)

async def GrossSales(grosssales_file_path):
    particulars_debit_df= await extract_Gross_Sales_data(grosssales_file_path)
    GrandtotalCredit_grosssales=particulars_debit_df['credit'][6]
    GrandtotalDebit_grosssales=particulars_debit_df['debit'][6]
    GrandtotalCredit_float = float(GrandtotalCredit_grosssales.replace(',', ''))
    GrandtotalDebit_float = float(GrandtotalDebit_grosssales.replace(',',''))
    grosssales = GrandtotalCredit_float - GrandtotalDebit_float

    result_json = {'GrossSales': grosssales}
    return json.dumps(result_json)

async def Discount_Disallowed(Disscount_file_path,Disscount_Disallwod_file_path):
    Grandtotal_float_discount= await extract_discount(Disscount_file_path)
    Grandtotal_float_discount_disallwod= await extract_discount_disallowed(Disscount_Disallwod_file_path)
    Discount_Disallowed=Grandtotal_float_discount+Grandtotal_float_discount_disallwod

    result_json = {'Discount_Disallowed': Discount_Disallowed}
    return json.dumps(result_json)

async def Net_Buisness(grosssales_file_path,Disscount_file_path,Disscount_Disallwod_file_path):
    Grandtotal_float_discount= await extract_discount(Disscount_file_path)
    Grandtotal_float_discount_disallwod= await extract_discount_disallowed(Disscount_Disallwod_file_path)
    particulars_debit_df= await extract_Gross_Sales_data(grosssales_file_path)
    GrandtotalCredit_grosssales=particulars_debit_df['credit'][6]
    GrandtotalDebit_grosssales=particulars_debit_df['debit'][6]
    GrandtotalCredit_float = float(GrandtotalCredit_grosssales.replace(',', ''))
    GrandtotalDebit_float = float(GrandtotalDebit_grosssales.replace(',',''))
    grosssales = GrandtotalCredit_float - GrandtotalDebit_float
    Net_Buisness=grosssales-Grandtotal_float_discount+Grandtotal_float_discount_disallwod

    result_json = {'Net_Buisness': Net_Buisness}
    return json.dumps(result_json)

async def combine_json_objects(json_list):
    combined_json = {}

    for index, json_data in enumerate(json_list, start=1):
        title = f"Card{index}"

        if isinstance(json_data, str):
            json_obj = json.loads(json_data)  # Convert the JSON string to a dictionary
            values_as_int = {key: int(value) if isinstance(value, str) and value.isdigit() else value for key, value in json_obj.items()}
            combined_json[title] = values_as_int
        elif isinstance(json_data, dict):
            # If it's already a dictionary, use it as is
            combined_json[title] = json_data
        else:
            raise ValueError(f"Invalid data type: {type(json_data)}")

    return json.dumps(combined_json, indent=2)
async def main():
    file_path= "C:/Users/jaint/Downloads/final1.xls"
    file_path1 = 'C:/Users/jaint/Downloads/AshaSalesVoucher.html'
    file_path2 = 'C:/Users/jaint/Downloads/AshaDirectExpenses.html'
    interestnfinacial_file_path = "C:/Users/jaint/Downloads/Interest&Finacial.html"
    Depreciaiton_file_path = "C:/Users/jaint/Downloads/depreciation.html"
    netprofit_file_path = "C:/Users/jaint/Downloads/3.html"
    grosssales_file_path = "C:/Users/jaint/Downloads/grosssales.html"
    Disscount_file_path="C:/Users/jaint/Downloads/Discount.html"
    Disscount_Disallwod_file_path = "C:/Users/jaint/Downloads/Disscount&Disallwod.html"



    dataframe = await DataFrame(file_path) 
    dataframe1 = await Dataframe1(file_path1)
    dataframe2 = await Dataframe2(file_path2)

    # storing card json:
    json1 = await Card1(dataframe1)
    json2 = await Card2(dataframe1)
    json3 = await Card3(dataframe2)
    json4 = await Card4(netprofit_file_path,Depreciaiton_file_path,interestnfinacial_file_path)
    json5 = await Card5(dataframe)
    json6 = await Card6(dataframe)
    json7 = await Card7(dataframe2)
    json8 = await Card8(dataframe1,dataframe2)
    grosssales= await GrossSales(grosssales_file_path)
    discount_disallowed = await Discount_Disallowed(Disscount_file_path,Disscount_Disallwod_file_path)
    net_Buisness = await Net_Buisness(grosssales_file_path,Disscount_file_path, Disscount_Disallwod_file_path)

    json_list = [json1, json2, json3, json4, json5, json6, json7,json8,grosssales,discount_disallowed,net_Buisness]

    result = await combine_json_objects(json_list)
    print(result)

if __name__ == "__main__":
    asyncio.run(main())