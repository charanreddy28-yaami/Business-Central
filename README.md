# Business-Central
This repository contains an automated ingestion pipeline that extracts data from the Microsoft Dynamics 365 Business Central API and stores it in Azure Data Lake Storage Gen2 (Blob Storage). The pipeline follows an incremental extraction approach to capture only new or updated records, while also supporting full data extraction when filters are not applied. Data from each Business Central object endpoint is saved as a separate CSV file and uploaded to ADLS Gen2. Downstream pipelines are triggered automatically based on file availability.

import numpy as np
import re
import pyodbc
from datetime import datetime
from datetime import date
pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 1000)

## Connect to Database
with open(r"config.json") as in_fil:
    json_conf = json.load(in_fil)

username = json_conf["username"]
password = json_conf["password"]
server = json_conf["server"]
databasename = json_conf["databasename"]
port = json_conf["port"]


driver = "ODBC+Driver+18+for+SQL+Server"
# Connection string
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=' +
            server+';DATABASE='+databasename+';UID='+username+';PWD=' + password)
query = f"SELECT ColumnName FROM [DatabaseName].[Config].[Tablename] WHERE ColumnName='Value'"
data = pd.read_sql(query, cnxn)
if not data.empty:
    formatted_date = pd.to_datetime(data['ColumnName'].iloc[0]).date()
    yesterday = (datetime.now() - timedelta(days=1)).date()
    if formatted_date == yesterday:
        print("The formatted_date matches yesterday's date.")

        ## Updating the ColumnName and Value column in the config table 
        update_query = f"""
        UPDATE [DatabaseName].[Config].[Tablename]
        SET ColumnName = 'value', ColumnName = 'value'
        WHERE ColumnName='value'
        """

        with cnxn.cursor() as cursor:
            cursor.execute(update_query)
            cnxn.commit()  


        import requests
        import json
        from datetime import datetime, timedelta
        import pandas as pd

# Step 1: Get the access token        
        url = "https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token"
        payload = 'client_id={Client_Id}&client_secret={Client_Secret}&scope=https%3A%2F%2Fapi.businesscentral.dynamics.com%2F.default&grant_type=client_credentials'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'Cookie': 'fpc=AlhyIgjUjfJPmKt_lYb7Y_yHW_oCAQAAAI3R4N0OAAAA; stsservicecookie=estsfd; x-ms-gateway-slice=estsfd'
        }

        response = requests.post(url, headers=headers, data=payload)
        if response.status_code == 200:
            access_token = response.json()['access_token']
        else:
            raise Exception(f"Failed to get access token: {response.text}")

        yesterday = datetime.now() - timedelta(days=1)
        yesterday_str = yesterday.strftime('%Y-%m-%d')

        # List of company IDs
        company_ids = ["68808132-2e66-ef11-a672-002248a6566c"]


        # Base URL for the Business Central API
        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{production}/api/v2.0/companies"

        # Headers including the authorization token
        headers = {
            'Authorization': f'Bearer {access_token}'  # Use an f-string to correctly insert the access token
        }

    
## items Table

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/items?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                items = response.json()


        df = pd.DataFrame(columns=['id','number','displayName', 'displayName2', 'type',
                                'itemCategoryId','itemCategoryCode','blocked','gtin','inventory',
                                'unitPrice','priceIncludesTax','unitCost','taxGroupId','taxGroupCode',
                                'baseUnitOfMeasureId','baseUnitOfMeasureCode','generalProductPostingGroupId',
                                'generalProductPostingGroupCode','inventoryPostingGroupId','inventoryPostingGroupCode',
                                'lastModifiedDateTime'])
        for i in items['value']:
            df.loc[len(df)] = [ 
                    i.get('id'),i.get('number'),
                    i.get("displayName"), i.get('displayName2'),i.get('type'),
                    i.get('itemCategoryId'),i.get('itemCategoryCode'),
                    i.get("blocked"), i.get('gtin'),i.get('inventory'),
                    i.get('unitPrice'),i.get('priceIncludesTax'),i.get("unitCost"),
                    i.get('taxGroupId'),i.get('taxGroupCode'),
                    i.get("baseUnitOfMeasureId"), i.get('baseUnitOfMeasureCode'),i.get('generalProductPostingGroupId'),
                    i.get('generalProductPostingGroupCode'),i.get('inventoryPostingGroupId'),i.get("inventoryPostingGroupCode"),
                    i.get("lastModifiedDateTime")
                ]
            
        df.to_json('items.json', orient='records', lines=True)


## Locations

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/locations?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                locations = response.json()


        df = pd.DataFrame(columns=['id','code','displayName', 'contact', 'addressLine1',
                                'addressLine2','city','state','country','postalCode',
                                'phoneNumber','email','website','lastModifiedDateTime'])
        for i in locations['value']:
            df.loc[len(df)] = [ 
                    i.get('id'),i.get('code'),
                    i.get("displayName"), i.get('contact'),i.get('addressLine1'),
                    i.get('addressLine2'),i.get('city'),
                    i.get("state"), i.get('country'),i.get('postalCode'),
                    i.get('phoneNumber'),i.get('email'),i.get('website'),i.get('lastModifiedDateTime')
                ]

        df.to_json('locations.json', orient='records', lines=True)


## itemCategories

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/itemCategories?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                itemCategories = response.json()

        df = pd.DataFrame(columns=[
            'id', 'code', 'displayName', 'lastModifiedDateTime'
        ])

        for i in itemCategories['value']:
            df.loc[len(df)] = [ 
                i.get('id'), i.get('code'), i.get('displayName'), i.get('lastModifiedDateTime')
            ]

        df.to_json('itemCategories.json', orient='records', lines=True)

        ## currencyExchangeRates

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/currencyExchangeRates?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                currencyExchangeRates = response.json()

        df = pd.DataFrame(columns=['id','currencyCode','startingDate', 'exchangeRateAmount', 'relationalCurrencyCode', 
                                'relationalExchangeRateAmount', 'lastModifiedDateTime'])
        for i in currencyExchangeRates['value']:
            df.loc[len(df)] = [ 
                    i.get('id'),i.get('currencyCode'),
                    i.get("startingDate"), i.get('exchangeRateAmount'), i.get('relationalCurrencyCode'),
                    i.get('relationalExchangeRateAmount'), i.get('lastModifiedDateTime')
                ]

        df.to_json('currencyExchangeRates.json', orient='records', lines=True)            

## currencies

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/currencies?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                currencies = response.json()

        df = pd.DataFrame(columns=['id','code','displayName', 'symbol', 'amountDecimalPlaces', 
                                'amountRoundingPrecision', 'lastModifiedDateTime'])
        for i in currencies['value']:
            df.loc[len(df)] = [ 
                    i.get('id'),i.get('code'),
                    i.get("displayName"), i.get('symbol'), i.get('amountDecimalPlaces'),
                    i.get('amountRoundingPrecision'), i.get('lastModifiedDateTime')
                ]        
            
        df.to_json('currencies.json', orient='records', lines=True)


## salesInvoices

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/salesInvoices?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                salesInvoices = response.json()

        df = pd.DataFrame(columns=[
            'id', 'number', 'externalDocumentNumber', 'invoiceDate','postingDate', 'dueDate', 
            'promisedPayDate', 'customerPurchaseOrderReference', 'customerId','customerNumber','customerName',
            'billToName', 'billToCustomerId','billToCustomerNumber', 'shipToName', 'shipToContact', 'sellToAddressLine1',
            'sellToAddressLine2', 'sellToCity', 'sellToCountry', 'sellToState',
            'sellToPostCode', 'billToAddressLine1', 'billToAddressLine2', 'billToCity', 'billToCountry',    
            'billToState', 'billToPostCode', 'shipToAddressLine1', 'shipToAddressLine2',
            'shipToCity', 'shipToCountry', 'shipToState', 'shipToPostCode', 'currencyId',
            'shortcutDimension1Code','shortcutDimension2Code','currencyCode','orderId',        
            'orderNumber', 'paymentTermsId','shipmentMethodId', 'salesperson','disputeStatusId','disputeStatus',
            'pricesIncludeTax','remainingAmount','discountAmount','discountAppliedBeforeTax','totalAmountExcludingTax',
            'totalTaxAmount','totalAmountIncludingTax','status','lastModifiedDateTime','phoneNumber','email'
        ])
        for i in salesInvoices['value']:
                df.loc[len(df)] = [ 
                    i.get('id'), i.get('number'), i.get('externalDocumentNumber'), i.get('invoiceDate'),i.get('postingDate'), 
                    i.get('dueDate'), i.get('promisedPayDate'), i.get('customerPurchaseOrderReference'),
                    i.get('customerId'),i.get('customerNumber'), i.get('customerName'), 
                    i.get('billToName'), i.get('billToCustomerId'), i.get('billToCustomerNumber'), i.get('shipToName'), 
                    i.get('shipToContact'), i.get('sellToAddressLine1'),i.get('sellToAddressLine2'), i.get('sellToCity'), 
                    i.get('sellToCountry'), i.get('sellToState'),i.get('sellToPostCode'),    
                    i.get('billToAddressLine1'), i.get('billToAddressLine2'), i.get('billToCity'), 
                    i.get('billToCountry'), i.get('billToState'), i.get('billToPostCode'), 
                    i.get('shipToAddressLine1'), i.get('shipToAddressLine2'), i.get('shipToCity'), 
                    i.get('shipToCountry'), i.get('shipToState') ,i.get('shipToPostCode'),i.get('currencyId'),
                    i.get('shortcutDimension1Code'),i.get('shortcutDimension2Code'),i.get('currencyCode'),i.get('orderId'),
                    i.get('orderNumber'),i.get('paymentTermsId'), i.get('shipmentMethodId'), i.get('salesperson'), 
                    i.get('disputeStatusId'), i.get('disputeStatus'),i.get('pricesIncludeTax'), i.get('remainingAmount'),
                    i.get('discountAmount'), i.get('discountAppliedBeforeTax'), i.get('totalAmountExcludingTax'), 
                    i.get('totalTaxAmount'),i.get('totalAmountIncludingTax'), i.get('status'), 
                    i.get('lastModifiedDateTime'), i.get('phoneNumber'), i.get('email')
                ]

        df.to_json('salesInvoices.json', orient='records', lines=True)

## salesInvoiceLines

        all_sales_invoice_lines = []

        # Iterate through each company
        for company_id in company_ids:
            # Construct the URL for retrieving sales invoices
            invoices_url = f"{base_url}({company_id})/salesInvoices?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            response = requests.get(invoices_url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                sales_invoices = response.json()
                sales_invoices_ids = [invoice['id'] for invoice in sales_invoices['value']]
                
                # Iterate through each sales invoice ID
                for sales_invoice_id in sales_invoices_ids:
                    # Construct the URL for the specific sales invoice lines
                    lines_url = f"{base_url}({company_id})/salesInvoices({sales_invoice_id})/salesInvoiceLines"
                    
                    response = requests.get(lines_url, headers=headers)
                    
                    # Check if the request was successful
                    if response.status_code == 200:
                        # Parse the JSON response
                        sales_invoice_lines = response.json()
                        # Append the lines to the list
                        all_sales_invoice_lines.extend(sales_invoice_lines['value'])
                    else:
                        print(f"Failed to retrieve sales invoice lines for Company ID {company_id} and Sales Invoice ID {sales_invoice_id}: {response.status_code} - {response.text}")
            else:
                print(f"Failed to retrieve sales invoices for Company ID {company_id}: {response.status_code} - {response.text}")


        df = pd.DataFrame(columns=[
            'id', 'documentId', 'sequence', 'itemId','accountId', 'lineType', 'lineObjectNumber', 
            'description', 'description2', 'unitOfMeasureId', 'unitOfMeasureCode',
            'quantity', 'discountAmount', 'discountPercent', 'discountAppliedBeforeTax',
            'amountExcludingTax', 'taxCode', 'taxPercent', 'totalTaxAmount', 'amountIncludingTax',
            'invoiceDiscountAllocation', 'netAmount', 'netTaxAmount', 'netAmountIncludingTax',
            'shipmentDate','itemVariantId', 'locationId'
        ])
        for i in all_sales_invoice_lines:
                df.loc[len(df)] = [ 
                    i.get('id'), i.get('documentId'), i.get('sequence'), i.get('itemId'),i.get('accountId'), 
                    i.get('lineType'), i.get('lineObjectNumber'), i.get('description'), 
                    i.get('description2'), i.get('unitOfMeasureId'), i.get('unitOfMeasureCode'), 
                    i.get('quantity'), i.get('discountAmount'), 
                    i.get('discountPercent'), i.get('discountAppliedBeforeTax'), 
                    i.get('amountExcludingTax'), i.get('taxCode'), i.get('taxPercent'), 
                    i.get('totalTaxAmount'), i.get('amountIncludingTax'), 
                    i.get('invoiceDiscountAllocation'), i.get('netAmount'), i.get('netTaxAmount'), 
                    i.get('netAmountIncludingTax'), i.get('shipmentDate'), 
                    i.get('itemVariantId'), i.get('locationId')
                ]

        df.to_json('salesInvoiceLines.json', orient='records', lines=True)

## salesCreditMemos

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/salesCreditMemos?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                salesCreditMemos = response.json()

        # Define the DataFrame with the correct column names
        df = pd.DataFrame(columns=[
            'id', 'number', 'externalDocumentNumber', 'creditMemoDate', 'postingDate', 'dueDate', 'customerId',
            'customerNumber', 'customerName', 'billToName', 'billToCustomerId', 'billToCustomerNumber',
            'sellToAddressLine1', 'sellToAddressLine2', 'sellToCity', 'sellToCountry', 'sellToState',
            'sellToPostCode', 'billToAddressLine1', 'billToAddressLine2', 'billToCity', 'billToCountry',
            'billToState', 'billToPostCode', 'shortcutDimension1Code', 'shortcutDimension2Code', 'currencyId',
            'currencyCode', 'paymentTermsId', 'shipmentMethodId', 'salesperson', 'pricesIncludeTax',
            'discountAmount', 'discountAppliedBeforeTax', 'totalAmountExcludingTax', 'totalTaxAmount',
            'totalAmountIncludingTax', 'status', 'lastModifiedDateTime', 'invoiceId', 'invoiceNumber',
            'phoneNumber', 'email', 'customerReturnReasonId'
        ])

        # Populate the DataFrame with data from salesCreditMemos
        for i in salesCreditMemos['value']:
            df.loc[len(df)] = [ 
                i.get('id'), i.get('number'), i.get('externalDocumentNumber'), i.get('creditMemoDate'),
                i.get('postingDate'), i.get('dueDate'), i.get('customerId'), i.get('customerNumber'),
                i.get('customerName'), i.get('billToName'), i.get('billToCustomerId'), i.get('billToCustomerNumber'),
                i.get('sellToAddressLine1'), i.get('sellToAddressLine2'), i.get('sellToCity'), i.get('sellToCountry'),
                i.get('sellToState'), i.get('sellToPostCode'), i.get('billToAddressLine1'), i.get('billToAddressLine2'),
                i.get('billToCity'), i.get('billToCountry'), i.get('billToState'), i.get('billToPostCode'),
                i.get('shortcutDimension1Code'), i.get('shortcutDimension2Code'), i.get('currencyId'),
                i.get('currencyCode'), i.get('paymentTermsId'), i.get('shipmentMethodId'), i.get('salesperson'),
                i.get('pricesIncludeTax'), i.get('discountAmount'), i.get('discountAppliedBeforeTax'),
                i.get('totalAmountExcludingTax'), i.get('totalTaxAmount'), i.get('totalAmountIncludingTax'),
                i.get('status'), i.get('lastModifiedDateTime'), i.get('invoiceId'), i.get('invoiceNumber'),
                i.get('phoneNumber'), i.get('email'), i.get('customerReturnReasonId')
            ]

        df.to_json('salesCreditMemos.json', orient='records', lines=True)

## salesCreditMemoLines

        all_purchase_invoice_lines = []

        # Iterate through each company
        for company_id in company_ids:
            # Construct the URL for retrieving purchase invoices
            invoices_url = f"{base_url}({company_id})/salesCreditMemos?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            response = requests.get(invoices_url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                purchase_invoices = response.json()
                purchaseInvoices_ids = [invoice['id'] for invoice in purchase_invoices['value']]
                
                # Iterate through each purchase invoice ID
                for purchaseInvoice_id in purchaseInvoices_ids:
                    print(purchaseInvoice_id)
                    # Construct the URL for the specific purchase invoice lines
                    lines_url = f"{base_url}({company_id})/salesCreditMemos({purchaseInvoice_id})/salesCreditMemoLines"
                    
                    response = requests.get(lines_url, headers=headers)
                    
                    # Check if the request was successful
                    if response.status_code == 200:
                        # Parse the JSON response
                        purchaseInvoiceLines = response.json()
                        # Append the lines to the list
                        all_purchase_invoice_lines.append(purchaseInvoiceLines)
                    else:
                        print(f"Failed to retrieve purchase invoice lines for Company ID {company_id} and Purchase Invoice ID {purchaseInvoice_id}: {response.status_code} - {response.text}")
            else:
                print(f"Failed to retrieve purchase invoices for Company ID {company_id}: {response.status_code} - {response.text}")

        # The variable `all_purchase_invoice_lines` now contains all the purchase invoice lines
        print(json.dumps(all_purchase_invoice_lines, indent=4))

        # Define the DataFrame with the correct column names
        df = pd.DataFrame(columns=[
            'id', 'documentId', 'sequence', 'itemId', 'accountId', 'lineType', 'lineObjectNumber',
            'description', 'description2', 'unitOfMeasureId', 'unitOfMeasureCode', 'unitPrice',
            'quantity', 'discountAmount', 'discountPercent', 'discountAppliedBeforeTax', 'amountExcludingTax',
            'taxCode', 'taxPercent', 'totalTaxAmount', 'amountIncludingTax', 'invoiceDiscountAllocation',
            'netAmount', 'netTaxAmount', 'netAmountIncludingTax', 'shipmentDate', 'itemVariantId',
            'locationId'
        ])

        # Populate the DataFrame with data from all_item_categories['value']
        for invoice in all_purchase_invoice_lines:
            # The 'value' key contains the actual list of lines
            for i in invoice.get('value', []):
                df.loc[len(df)] = [
                i.get('id'), i.get('documentId'), i.get('sequence'), i.get('itemId'), i.get('accountId'),
                i.get('lineType'), i.get('lineObjectNumber'), i.get('description'), i.get('description2'),
                i.get('unitOfMeasureId'), i.get('unitOfMeasureCode'), i.get('unitPrice'), i.get('quantity'),
                i.get('discountAmount'), i.get('discountPercent'), i.get('discountAppliedBeforeTax'),
                i.get('amountExcludingTax'), i.get('taxCode'), i.get('taxPercent'), i.get('totalTaxAmount'),
                i.get('amountIncludingTax'), i.get('invoiceDiscountAllocation'), i.get('netAmount'),
                i.get('netTaxAmount'), i.get('netAmountIncludingTax'), i.get('shipmentDate'), i.get('itemVariantId'),
                i.get('locationId')
            ]

        df.to_json('salesCreditMemoLines.json', orient='records', lines=True)

## Vendors

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/vendors?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"

            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                vendors = response.json()

        df = pd.DataFrame(columns=['id','number','displayName', 'addressLine1', 'addressLine2', 'city',
                                'state', 'country', 'postalCode', 'phoneNumber', 'email',
                                'website', 'taxRegistrationNumber', 'currencyId', 'currencyCode',
                                'irs1099Code', 'paymentTermsId', 'taxLiable', 'blocked', 'balance',
                                'lastModifiedDateTime'
                                ])
        for i in vendors['value']:
            df.loc[len(df)] = [ 
                    i.get('id'),i.get('number'),
                    i.get("displayName"), i.get('addressLine1'), i.get('addressLine2'),
                    i.get('city'),i.get("state"), i.get('country'), i.get('postalCode'),
                    i.get("phoneNumber"), i.get('email'), i.get('website'),
                    i.get("taxRegistrationNumber"), i.get('currencyId'), i.get('currencyCode'),
                    i.get("irs1099Code"), i.get('paymentTermsId'), i.get('taxLiable'),
                    i.get("blocked"), i.get('balance'), i.get('lastModifiedDateTime'),
                ]

        df.to_json('vendors.json', orient='records', lines=True)

## vendorscontactsInformation

        all_item_categories = []

        # Iterate through each company
        for company_id in company_ids:
            # Construct the URL for retrieving items
            items_url = f"{base_url}({company_id})/vendors?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            response = requests.get(items_url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                items = response.json()
                item_ids = [item['id'] for item in items['value']]
                
                # Iterate through each item ID
                for item_id in item_ids:
                    # Construct the URL for the specific item category
                    item_category_url = f"{base_url}({company_id})/vendors({item_id})/contactsInformation"
                    
                    response = requests.get(item_category_url, headers=headers)
                    
                    # Check if the request was successful
                    if response.status_code == 200:
                        # Parse the JSON response
                        item_category = response.json()
                        # Append the item category data to the list
                        all_item_categories.append(item_category)
                    else:
                        print(f"Failed to retrieve item category for Company ID {company_id} and Item ID {item_id}: {response.status_code} - {response.text}")
            else:
                print(f"Failed to retrieve items for Company ID {company_id}: {response.status_code} - {response.text}")

        # Define the DataFrame with the correct column names
        df = pd.DataFrame(columns=[
            'contactId', 'contactNumber', 'contactName', 'contactType','relatedId','relatedType'
        ])

        # Populate the DataFrame with data from salesCreditMemos
        for invoice in all_item_categories:
            # The 'value' key contains the actual list of lines
            for i in invoice.get('value', []):    
                df.loc[len(df)] = [ 
                i.get('contactId'), i.get('contactNumber'), i.get('contactName'), i.get('contactType'),
                i.get('relatedId'), i.get('relatedType')
            ]

        df.to_json('vendContact.json', orient='records', lines=True)


## PurchaseInvoices

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/purchaseInvoices?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            print(yesterday_str)

            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                purchaseInvoices = response.json()


        df = pd.DataFrame(columns=[
            'id', 'number','postingDate','invoiceDate','dueDate','vendorInvoiceNumber','vendorId', 
            'vendorNumber', 'vendorName', 'payToName','payToContact','payToVendorId','payToVendorNumber',
            'shipToName', 'shipToContact', 'buyFromAddressLine1',
            'buyFromAddressLine2', 'buyFromCity', 'buyFromCountry', 'buyFromState',
            'buyFromPostCode', 'shipToAddressLine1', 'shipToAddressLine2',
            'shipToCity', 'shipToCountry', 'shipToState', 'shipToPostCode','payToAddressLine1',
            'payToAddressLine2','payToCity','payToCountry','payToState','payToPostCode',
            'shortcutDimension1Code','shortcutDimension2Code','currencyId','currencyCode', 'orderId','orderNumber',     
            'purchaser','pricesIncludeTax','discountAmount',
            'discountAppliedBeforeTax','totalAmountExcludingTax','totalTaxAmount','totalAmountIncludingTax',
            'status','lastModifiedDateTime'])
        for i in purchaseInvoices['value']:
                df.loc[len(df)] = [ 
                    i.get('id'), i.get('number'), i.get('postingDate'),i.get('invoiceDate'),i.get('dueDate'), 
                    i.get('vendorInvoiceNumber'),i.get('vendorId'), i.get('vendorNumber'), i.get('vendorName'),
                    i.get('payToName'),i.get('payToContact'),i.get('payToVendorId'), i.get('payToVendorNumber'), 
                    i.get('shipToName'), i.get('shipToContact'), i.get('buyFromAddressLine1'), i.get('buyFromAddressLine2'),     
                    i.get('buyFromCity'), i.get('buyFromCountry'),i.get('buyFromState'), i.get('buyFromPostCode'), 
                    i.get('shipToAddressLine1'), i.get('shipToAddressLine2'), i.get('shipToCity'), 
                    i.get('shipToCountry'), i.get('shipToState') ,i.get('shipToPostCode'),
                    i.get('payToAddressLine1'), i.get('payToAddressLine2'),i.get('payToCity'),    
                    i.get('payToCountry'), i.get('payToState'), i.get('payToPostCode'),
                    i.get('shortcutDimension1Code'),i.get('shortcutDimension2Code'),i.get('currencyId'),i.get('currencyCode'),   
                    i.get('orderId'),i.get('orderNumber'),i.get('purchaser'),i.get('pricesIncludeTax'),
                    i.get('discountAmount'),i.get('discountAppliedBeforeTax'),
                    i.get('totalAmountExcludingTax'),i.get('totalTaxAmount'), i.get('totalAmountIncludingTax'), 
                    i.get('status'),i.get('lastModifiedDateTime')]

        df.to_json('purchaseInvoices.json', orient='records', lines=True)

## purchaseInvoiceLines

        all_purchase_order_lines = []

        # Iterate through each company
        for company_id in company_ids:
            # Construct the URL for retrieving purchase orders
            orders_url = f"{base_url}({company_id})/purchaseInvoices?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            response = requests.get(orders_url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                purchase_orders = response.json()
                purchase_orders_ids = [order['id'] for order in purchase_orders['value']]
                
                # Iterate through each purchase order ID
                for purchase_order_id in purchase_orders_ids:
                    print(purchase_order_id)
                    # Construct the URL for the specific purchase order lines
                    lines_url = f"{base_url}({company_id})/purchaseInvoices({purchase_order_id})/purchaseInvoiceLines"
                    
                    response = requests.get(lines_url, headers=headers)
                    
                    # Check if the request was successful
                    if response.status_code == 200:
                        # Parse the JSON response
                        purchase_order_lines = response.json()
                        # Append the lines to the list
                        all_purchase_order_lines.extend(purchase_order_lines['value'])
                    else:
                        print(f"Failed to retrieve purchase order lines for Company ID {company_id} and Purchase Order ID {purchase_order_id}: {response.status_code} - {response.text}")
            else:
                print(f"Failed to retrieve purchase orders for Company ID {company_id}: {response.status_code} - {response.text}")

        # Create an empty DataFrame with the specified columns
        df = pd.DataFrame(columns=[
            'id', 'documentId', 'sequence', 'accountId', 'lineType', 'lineObjectNumber', 
            'description', 'description2', 'unitOfMeasureId', 'unitOfMeasureCode', 'unitCost',
            'quantity', 'discountAmount', 'discountPercent', 'discountAppliedBeforeTax',
            'amountExcludingTax', 'taxCode', 'taxPercent', 'totalTaxAmount', 'amountIncludingTax',
            'invoiceDiscountAllocation', 'netAmount', 'netTaxAmount', 'netAmountIncludingTax',
            'expectedReceiptDate', 'itemVariantId', 'locationId'
        ])

        # Iterate over the list of dictionaries in all_purchase_invoice_lines
        for i in all_purchase_order_lines:
                df.loc[len(df)] = [ 
                    i.get('id'), i.get('documentId'), i.get('sequence'), i.get('accountId'), 
                    i.get('lineType'), i.get('lineObjectNumber'), i.get('description'), 
                    i.get('description2'), i.get('unitOfMeasureId'), i.get('unitOfMeasureCode'), 
                    i.get('unitCost'), i.get('quantity'), i.get('discountAmount'), 
                    i.get('discountPercent'), i.get('discountAppliedBeforeTax'), 
                    i.get('amountExcludingTax'), i.get('taxCode'), i.get('taxPercent'), 
                    i.get('totalTaxAmount'), i.get('amountIncludingTax'), 
                    i.get('invoiceDiscountAllocation'), i.get('netAmount'), i.get('netTaxAmount'), 
                    i.get('netAmountIncludingTax'), i.get('expectedReceiptDate'), 
                    i.get('itemVariantId'), i.get('locationId')
                ]

        df.to_json('purchaseInvoiceLines.json', orient='records', lines=True)

## GeneralLedgerEntries

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/generalLedgerEntries?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"

            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                generalLedgerEntries = response.json()

        df = pd.DataFrame(columns=['id','entryNumber','postingDate', 'documentNumber', 'documentType',
                                'accountId','accountNumber','description','debitAmount','creditAmount',
                                'additionalCurrencyDebitAmount','additionalCurrencyCreditAmount','lastModifiedDateTime'])
        for i in generalLedgerEntries['value']:
            df.loc[len(df)] = [ 
                    i.get('id'),i.get('entryNumber'),
                    i.get("postingDate"), i.get('documentNumber'),i.get('documentType'),
                    i.get('accountId'),i.get('accountNumber'),
                    i.get("description"), i.get('debitAmount'),i.get('creditAmount'),
                    i.get('additionalCurrencyDebitAmount'),i.get('additionalCurrencyCreditAmount'),
                    i.get("lastModifiedDateTime")
                ]

        df.to_json('generalLedgerEntries.json', orient='records', lines=True)

## paymentTerms
        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/paymentTerms"
            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                paymentTerms = response.json()

        df = pd.DataFrame(paymentTerms['value'])   
        df = df.drop(columns=['@odata.etag'])
        df.to_json('paymentTerms.json', orient='records', lines=True) 


## unitsOfMeasure
        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/unitsOfMeasure"
            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                unitsOfMeasure = response.json()

        df = pd.DataFrame(unitsOfMeasure['value'])   
        df = df.drop(columns=['@odata.etag'])
        df.to_json('unitsOfMeasure.json', orient='records', lines=True)    

## Accounts

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/accounts?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"   
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers) 
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                accounts = response.json()


        import pandas as pd
        df = pd.DataFrame(columns=['Accountid','AccountNumber','DisplayName', 'category', 'subCategory', 'blocked', 'accountType',
                                'directPosting', 'netChange', 'consolidationTranslationMethod','consolidationDebitAccount',
                                'consolidationCreditAccount','excludeFromConsolidation','lastModifiedDateTime'])
        for i in accounts['value']:
            df.loc[len(df)] = [ 
                    i.get('id'),i.get('number'),
                    i.get("displayName"), i.get('category'), i.get('subCategory'),
                    i.get('blocked'), i.get('accountType'),i.get('directPosting'), i.get('netChange'),
                    i.get('consolidationTranslationMethod'),i.get('consolidationDebitAccount'), i.get('consolidationCreditAccount'),
                    i.get('excludeFromConsolidation'),i.get('lastModifiedDateTime')
                ]
        df.to_json('Accounts.json', orient='records', lines=True)

        # ## trialBalances

        # from datetime import datetime, timedelta
        # from dateutil.relativedelta import relativedelta


        # today = datetime.now()

        # # Calculate the first day of the month three months ago
        # three_months_ago = today.replace(day=1) - relativedelta(months=3)

        # # End date is yesterday
        # end_date = today - timedelta(days=1)

        # # Print the calculated start and end dates
        # print(f"Start date (three months ago): {three_months_ago}")
        # print(f"End date (yesterday): {end_date}")

        # # List to store JSON responses
        # all_responses = []

        # # Loop through each company
        # for company_id in company_ids:
        #     current_date = three_months_ago
        #     while current_date <= end_date:
        #         # Set the date for the request
        #         date_str = current_date.strftime('%Y-%m-%d')

        #         # Construct the URL for the specific company and date
        #         url = f"{base_url}({company_id})/trialBalances?$filter=dateFilter ge {date_str} and dateFilter le {date_str}"

        #         # Make the API request
        #         response = requests.get(url, headers=headers)

        #         # Check if the request was successful
        #         if response.status_code == 200:
        #             # Parse the JSON response and append to the list
        #             journal = response.json()
        #             all_responses.append(journal['value'])
        #         else:
        #             print(f"Failed to retrieve data for company {company_id} on {date_str}: {response.status_code} - {response.text}")

        #         # Move to the next day
        #         current_date += timedelta(days=1)

        # # Step 4: Create a DataFrame from the responses
        # df = pd.DataFrame(columns=['accountId', 'number', 'accountType', 'display', 'totalDebit', 'totalCredit',
        #                            'balanceAtDateDebit', 'balanceAtDateCredit', 'dateFilter'])

        # for response_list in all_responses:
        #     for i in response_list:
        #         df.loc[len(df)] = [
        #             i.get('accountId'), i.get('number'),
        #             i.get("accountType"), i.get('display'), i.get('totalDebit'),
        #             i.get('totalCredit'), i.get('balanceAtDateDebit'), i.get('balanceAtDateCredit'), i.get('dateFilter')
        #         ]

        # # Replace empty values in the specified columns
        # columns_to_update = ['totalDebit', 'totalCredit', 'balanceAtDateDebit', 'balanceAtDateCredit']
        # df[columns_to_update] = df[columns_to_update].replace("", "0.00")

        # # Step 5: Export the DataFrame to JSON
        # df.to_json('TrialBalances.json', orient='records', lines=True)

## salesShipments

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/salesShipments?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                salesShipments = response.json()

        df = pd.DataFrame(salesShipments['value'])
        if '@odata.etag' in df.columns:
            df = df.drop(columns=['@odata.etag'])
        df.to_json('salesShipments.json', orient='records', lines=True)

## salesShipmentLines

        all_purchase_order_lines = []

        # Iterate through each company
        for company_id in company_ids:
            # Construct the URL for retrieving purchase orders
            orders_url = f"{base_url}({company_id})/salesShipments?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            response = requests.get(orders_url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                purchase_orders = response.json()
                purchase_orders_ids = [order['id'] for order in purchase_orders['value']]
                
                # Iterate through each purchase order ID
                for purchase_order_id in purchase_orders_ids:
                    print(purchase_order_id)
                    # Construct the URL for the specific purchase order lines
                    lines_url = f"{base_url}({company_id})/salesShipments({purchase_order_id})/salesShipmentLines"
                    
                    response = requests.get(lines_url, headers=headers)
                    
                    # Check if the request was successful
                    if response.status_code == 200:
                        # Parse the JSON response
                        purchase_order_lines = response.json()
                        # Append the lines to the list
                        all_purchase_order_lines.extend(purchase_order_lines['value'])
                    else:
                        print(f"Failed to retrieve purchase order lines for Company ID {company_id} and Purchase Order ID {purchase_order_id}: {response.status_code} - {response.text}")
            else:
                print(f"Failed to retrieve purchase orders for Company ID {company_id}: {response.status_code} - {response.text}")
                
        # Create an empty DataFrame with the specified columns
        df = pd.DataFrame(columns=[
            'id', 'documentId', 'documentNo', 'sequence', 'lineType', 'lineObjectNumber', 
            'description', 'description2', 'unitOfMeasureCode', 'unitPrice',
            'quantity', 'discountPercent', 'taxPercent', 'shipmentDate'
        ])

        # Iterate over the list of dictionaries in all_purchase_invoice_lines
        for i in all_purchase_order_lines:
                df.loc[len(df)] = [ 
                    i.get('id'), i.get('documentId'), i.get('documentNo'), i.get('sequence'), 
                    i.get('lineType'), i.get('lineObjectNumber'), i.get('description'), 
                    i.get('description2'), i.get('unitOfMeasureCode'), i.get('unitPrice'), 
                    i.get('quantity'), i.get('discountPercent'), i.get('taxPercent'), 
                    i.get('shipmentDate')
                ]

        df.to_json('salesShipmentLines.json', orient='records', lines=True)

## shipmentMethods

        for company_id in company_ids:
            # Construct the URL for the specific company
            url = f"{base_url}({company_id})/shipmentMethods?$filter=lastModifiedDateTime ge {yesterday_str}T00:00:00Z and lastModifiedDateTime lt {yesterday_str}T23:59:59Z"
            
            # Make the GET request to retrieve accounts for the current company
            response = requests.get(url, headers=headers)
            
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response
                shipmentMethods = response.json()

        df = pd.DataFrame(columns=['id', 'code', 'displayName', 'lastModifiedDateTime'])

        # Iterate over the list of dictionaries in all_purchase_invoice_lines
        for i in shipmentMethods['value']:
                df.loc[len(df)] = [ 
                    i.get('id'), i.get('code'), i.get('displayName'), i.get('lastModifiedDateTime')
                ]
        df.to_json('shipmentMethods.json', orient='records', lines=True)

##   WEBSERVICE ODATA ENDPOINTS DATA EXTRACTION

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/CustomersList"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('CustomerCard.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")

## Salespersons

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/Salespersons"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('Salespersons.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")    

## VendorCard
        base_url= "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})"
        url = f"{base_url}/VendorsList"
            
        # Make the GET request to retrieve accounts for the current company
        response = requests.get(url, headers=headers)
            
        # Check if the request was successful
        if response.status_code == 200:
            # Parse the JSON response
            VendorCard = response.json()

        values = VendorCard['value']

        df = pd.DataFrame(values)

        df = df.drop(columns=['@odata.etag'])

        df.to_json('VendorCard.json', orient='records', lines=True)


## ItemLedgerEntries

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/ItemLedgerEntries"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('ItemLedgerEntries.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")    

## PurchaseCreditMemo

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PurchaseCreditMemo"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PurchaseCreditMemo.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  


## PurchaseCreditMemoLine

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PurchaseCreditMemoLines"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PurchaseCreditMemoLines.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  
            
## PostedPurchaseCreditMemo


        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PostedPurchaseCreditMemo"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PostedPurchaseCreditMemo.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")    

## PostedPurchaseCrMemoLines


        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PostedPurchaseCrMemoLines"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PostedPurchaseCrMemoLines.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

        ## CustomerLedgerEntryDetails


        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/CustomerLedgerEntryDetails"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('CustomerLedgerEntryDetails.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## CustomerLedgerEntries


        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/CustomerLedgerEntries"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('CustomerLedgerEntries.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## VendorLedgerEntriesDetails

        # Construct the URL for the specific company

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/VendorLedgerEntriesDetails"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('VendorLedgerEntriesDetails.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## VendorLedgerEntries


        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/VendorLedgerEntries"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('VendorLedgerEntries.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## ValueEntries


        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/ValueEntries"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('ValueEntries.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## Chart of Accounts


        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/Chart_of_Accounts"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('ChartOfAccounts.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## PostedPurchaseReceipt


        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PostedPurchaseReceipt"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PostedPurchaseReceipt.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

# PostedPurchaseReceipts

        # base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PostedPurchaseReceipts"
        # # List to store all records
        # all_records = []

        # # Start with the base URL
        # url = base_url

        # # Loop to handle pagination and fetch all records
        # while url:
        #     response = requests.get(url, headers=headers)
        #     response.raise_for_status()  # Ensure we handle HTTP errors
        #     data = response.json()

        #     # Add current page of records to all_records
        #     all_records.extend(data.get('value', []))

        #     # Get the next page URL
        #     url = data.get('@odata.nextLink')

        # # Print total number of records retrieved
        # print(f"Total records retrieved: {len(all_records)}")

        # df = pd.DataFrame(all_records)

        # df = df.drop(columns=['@odata.etag'])

        # df.to_json('PostedPurchaseReceipts.json', orient='records', lines=True)

## PostedPurchaseReceiptLines


        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PostedPurchaseReceiptLines"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PostedPurchaseReceiptLines.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

        ## PostedReturnShipment

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PostedReturnShipment"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PostedReturnShipment.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")


## PostedReturnShipmentLines

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PostedReturnShipmentLines"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PostedReturnShipmentLines.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")

## PostedReturnReceipts

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PostedReturnReceipts"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PostedReturnReceipts.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")

## PostedReturnReceiptSubform

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PostedReturnReceiptSubform"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PostedReturnReceiptSubform.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")

## PurchaseHeader

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PurchaseHeader"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PurchaseHeader.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## PurchaseHeaderHistory

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PurchaseOrderHistory"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PurchaseHeaderHistory.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")

## PurchaseLine

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PurchaseLine"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PurchaseLine.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## PurchaseLineHistory

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/PurchaseLineHistory"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('PurchaseLineHistory.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")

## SalesLine

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/SalesLine"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('SalesLine.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## SalesLineHistory

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/SalesLineHistory"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('SalesLineHistory.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## SalesOrder

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/SalesOrder"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('SalesOrder.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

## SalesOrderHistory

        base_url = "https://api.businesscentral.dynamics.com/v2.0/{tenantId}/{environment}/ODataV4/Company(/{compayId})/SalesOrderHistory"

        # List to store all records
        all_records = []

        # Start with the base URL
        url = base_url

        try:
            # Loop to handle pagination and fetch all records
            while url:
                response = requests.get(url, headers=headers)
                response.raise_for_status()  # Raise error for HTTP issues
                data = response.json()

                # Add current page of records to all_records
                all_records.extend(data.get('value', []))

                # Get the next page URL
                url = data.get('@odata.nextLink')

            # Print total number of records retrieved
            print(f"Total records retrieved: {len(all_records)}")

            # Convert the records to a DataFrame
            df = pd.DataFrame(all_records)

            # Drop the '@odata.etag' column if it exists
            if '@odata.etag' in df.columns:
                df = df.drop(columns=['@odata.etag'])

            # Save to a JSON file
            df.to_json('SalesOrderHistory.json', orient='records', lines=True)

        except requests.exceptions.HTTPError as e:
            print(f"HTTP error occurred: {e}")
            print(f"Response content: {response.text}")
        except Exception as e:
            print(f"An error occurred: {e}")  

# uploading into blob
# Azure Storage Account connection string
        import datetime
        from azure.storage.blob import BlobServiceClient

        # Get current date information
        date_year = datetime.datetime.now().year
        date_month = datetime.datetime.now().strftime('%B')  # Change to "%B" to push to the current month
        date_format = datetime.datetime.now().strftime('%Y-%m-%d')

        from azure.storage.blob import BlobServiceClient, ContainerClient
        azure_storage_connection_string = "xxxxxx"
        container_name = "xxxx"

        def upload_to_blob(file_path, blob_name, container_name, azure_storage_connection_string):
            blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)
            
            container_client = blob_service_client.get_container_client(container_name)
            if not container_client.exists():
                container_client.create_container()

            with open(file_path, "rb") as data:
                container_client.upload_blob(name=blob_name, data=data)

        BusinessCentral_Path = f"Path/{date_year}/{date_month}/{date_format}"


        # Upload the generated JSON files with the specified dynamic path
        upload_to_blob("items.json", f"{BusinessCentral_Path}/items.json", container_name, azure_storage_connection_string)
        upload_to_blob("locations.json", f"{BusinessCentral_Path}/locations.json", container_name, azure_storage_connection_string)
        upload_to_blob("itemCategories.json", f"{BusinessCentral_Path}/itemCategories.json", container_name, azure_storage_connection_string)
        upload_to_blob("currencyExchangeRates.json", f"{BusinessCentral_Path}/currencyExchangeRates.json", container_name, azure_storage_connection_string)
        upload_to_blob("currencies.json", f"{BusinessCentral_Path}/currencies.json", container_name, azure_storage_connection_string)
        upload_to_blob("salesInvoices.json", f"{BusinessCentral_Path}/salesInvoices.json", container_name, azure_storage_connection_string)
        upload_to_blob("salesInvoiceLines.json", f"{BusinessCentral_Path}/salesInvoiceLines.json", container_name, azure_storage_connection_string)
        upload_to_blob("salesCreditMemos.json", f"{BusinessCentral_Path}/salesCreditMemos.json", container_name, azure_storage_connection_string)
        upload_to_blob("salesCreditMemoLines.json", f"{BusinessCentral_Path}/salesCreditMemoLines.json", container_name, azure_storage_connection_string)
        upload_to_blob("vendors.json", f"{BusinessCentral_Path}/vendors.json", container_name, azure_storage_connection_string)
        upload_to_blob("vendContact.json", f"{BusinessCentral_Path}/vendContact.json", container_name, azure_storage_connection_string)
        upload_to_blob("purchaseInvoices.json", f"{BusinessCentral_Path}/purchaseInvoices.json", container_name, azure_storage_connection_string)
        upload_to_blob("purchaseInvoiceLines.json", f"{BusinessCentral_Path}/purchaseInvoiceLines.json", container_name, azure_storage_connection_string)
        upload_to_blob("generalLedgerEntries.json", f"{BusinessCentral_Path}/generalLedgerEntries.json", container_name, azure_storage_connection_string)
        upload_to_blob("paymentTerms.json", f"{BusinessCentral_Path}/paymentTerms.json", container_name, azure_storage_connection_string)
        upload_to_blob("unitsOfMeasure.json", f"{BusinessCentral_Path}/unitsOfMeasure.json", container_name, azure_storage_connection_string)
        upload_to_blob("Accounts.json", f"{BusinessCentral_Path}/Accounts.json", container_name, azure_storage_connection_string)
        upload_to_blob("salesShipments.json", f"{BusinessCentral_Path}/salesShipments.json", container_name, azure_storage_connection_string)
        upload_to_blob("salesShipmentLines.json", f"{BusinessCentral_Path}/salesShipmentLines.json", container_name, azure_storage_connection_string)
        upload_to_blob("shipmentMethods.json", f"{BusinessCentral_Path}/shipmentMethods.json", container_name, azure_storage_connection_string)
        upload_to_blob("CustomerCard.json", f"{BusinessCentral_Path}/CustomerCard.json", container_name, azure_storage_connection_string)
        upload_to_blob("Salespersons.json", f"{BusinessCentral_Path}/Salespersons.json", container_name, azure_storage_connection_string)
        upload_to_blob("VendorCard.json", f"{BusinessCentral_Path}/VendorCard.json", container_name, azure_storage_connection_string)
        upload_to_blob("ItemLedgerEntries.json", f"{BusinessCentral_Path}/ItemLedgerEntries.json", container_name, azure_storage_connection_string)
        upload_to_blob("PurchaseCreditMemo.json", f"{BusinessCentral_Path}/PurchaseCreditMemo.json", container_name, azure_storage_connection_string)
        upload_to_blob("PurchaseCreditMemoLines.json", f"{BusinessCentral_Path}/PurchaseCreditMemoLines.json", container_name, azure_storage_connection_string)
        upload_to_blob("PostedPurchaseCreditMemo.json", f"{BusinessCentral_Path}/PostedPurchaseCreditMemo.json", container_name, azure_storage_connection_string)
        upload_to_blob("PostedPurchaseCrMemoLines.json", f"{BusinessCentral_Path}/PostedPurchaseCrMemoLines.json", container_name, azure_storage_connection_string)
        upload_to_blob("CustomerLedgerEntryDetails.json", f"{BusinessCentral_Path}/CustomerLedgerEntryDetails.json", container_name, azure_storage_connection_string)
        upload_to_blob("CustomerLedgerEntries.json", f"{BusinessCentral_Path}/CustomerLedgerEntries.json", container_name, azure_storage_connection_string)
        upload_to_blob("VendorLedgerEntriesDetails.json", f"{BusinessCentral_Path}/VendorLedgerEntriesDetails.json", container_name, azure_storage_connection_string)
        upload_to_blob("VendorLedgerEntries.json", f"{BusinessCentral_Path}/VendorLedgerEntries.json", container_name, azure_storage_connection_string)
        upload_to_blob("ValueEntries.json", f"{BusinessCentral_Path}/ValueEntries.json", container_name, azure_storage_connection_string)
        upload_to_blob("ChartOfAccounts.json", f"{BusinessCentral_Path}/ChartOfAccounts.json", container_name, azure_storage_connection_string)
        upload_to_blob("PostedPurchaseReceipt.json", f"{BusinessCentral_Path}/PostedPurchaseReceipt.json", container_name, azure_storage_connection_string)
        upload_to_blob("PostedPurchaseReceiptLines.json", f"{BusinessCentral_Path}/PostedPurchaseReceiptLines.json", container_name, azure_storage_connection_string)
        upload_to_blob("PostedReturnShipment.json", f"{BusinessCentral_Path}/PostedReturnShipment.json", container_name, azure_storage_connection_string)
        upload_to_blob("PostedReturnShipmentLines.json", f"{BusinessCentral_Path}/PostedReturnShipmentLines.json", container_name, azure_storage_connection_string)
        upload_to_blob("PostedReturnReceipts.json", f"{BusinessCentral_Path}/PostedReturnReceipts.json", container_name, azure_storage_connection_string)
        upload_to_blob("PostedReturnReceiptSubform.json", f"{BusinessCentral_Path}/PostedReturnReceiptSubform.json", container_name, azure_storage_connection_string)
        upload_to_blob("PurchaseHeader.json", f"{BusinessCentral_Path}/PurchaseHeader.json", container_name, azure_storage_connection_string)
        upload_to_blob("PurchaseHeaderHistory.json", f"{BusinessCentral_Path}/PurchaseHeaderHistory.json", container_name, azure_storage_connection_string)
        upload_to_blob("PurchaseLine.json", f"{BusinessCentral_Path}/PurchaseLine.json", container_name, azure_storage_connection_string)
        upload_to_blob("PurchaseLineHistory.json", f"{BusinessCentral_Path}/PurchaseLineHistory.json", container_name, azure_storage_connection_string)
        upload_to_blob("SalesLine.json", f"{BusinessCentral_Path}/SalesLine.json", container_name, azure_storage_connection_string)
        upload_to_blob("SalesLineHistory.json", f"{BusinessCentral_Path}/SalesLineHistory.json", container_name, azure_storage_connection_string)
        upload_to_blob("SalesOrder.json", f"{BusinessCentral_Path}/SalesOrder.json", container_name, azure_storage_connection_string)
        upload_to_blob("SalesOrderHistory.json", f"{BusinessCentral_Path}/SalesOrderHistory.json", container_name, azure_storage_connection_string)



        ## Uploading the Config File into Blob
        import datetime
        from azure.storage.blob import BlobServiceClient
        current_datetime = datetime.datetime.now()
        End_Time = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
        print(End_Time)

        def generate_and_upload_to_blob():
            current_datetime = datetime.datetime.now()
            date_format = current_datetime.strftime('%Y-%m-%d')
            azure_storage_connection_string = "xxxxx"
            container_name = "xxxx"
            def upload_to_blob(file_path, blob_name, container_name, azure_storage_connection_string):
                blob_service_client = BlobServiceClient.from_connection_string(azure_storage_connection_string)

                container_client = blob_service_client.get_container_client(container_name)
                if not container_client.exists():
                    container_client.create_container()

                with open(file_path, "rb") as data:
                    container_client.upload_blob(name=blob_name, data=data)

            file_name = "trigger_file{}.txt".format(date_format)
            file_path = file_name
            formatted_datetime = current_datetime.strftime("%Y-%m-%d_%H-%M-%S")
            with open(file_path, "w") as file:
                file.write("Run Completed Time: " + End_Time)
            print("Text file '{}' has been generated with the current date.".format(file_name))
            BusinessCentral_Path = f"path/Trigger/"
            upload_to_blob(file_path, f"{BusinessCentral_Path}/{file_name}", container_name, azure_storage_connection_string)
            print("File has been uploaded to Azure Blob Storage.")
        generate_and_upload_to_blob() 

        ## Updating the ColumnName column in the config table 
        update_query = f"""
        UPDATE [DatabaseName].[Config].[Tablename] 
        SET ColumnName = 'value', ColumnName = GETDATE()
        WHERE ColumnName='value'
        """
        with cnxn.cursor() as cursor:
            cursor.execute(update_query)
            cnxn.commit()   

    else:
        print("The formatted_date does not match yesterday's date.")
else:
    print("No data found for the specified erpkey.")       
