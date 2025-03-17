import http.client
import json
import csv
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path("venv/.env"))

def fetch_business_data():
    """
    Fetch business data from the RapidAPI Local Business Data API
    
    Returns:
        dict: The API response data or None if the request failed
    """
    try:
        conn = http.client.HTTPSConnection("local-business-data.p.rapidapi.com")
        
        # Get API key from environment variable
        api_key = os.environ.get('RAPIDAPI_KEY')
        api_host = os.environ.get('RAPIDAPI_HOST', 'local-business-data.p.rapidapi.com')
        
        headers = {
            'x-rapidapi-key': api_key,
            'x-rapidapi-host': api_host
        }
        
        query_path = "/search-in-area?query=Real%20estate%2C%20Real%20estate%20management%2C%20Interior%20Design&lat=52.520008&lng=13.404954&zoom=10&limit=100&language=en&region=de&subtypes=Real%20estate%2C%20Real%20estate%20agency%2C%20Real%20estate%20surveyor%2C%20Real%20estate%20developer%2C%20Architect%2C%20Apartment%20rental%20agency%2C%20Architectural%20designer%2C%20Architecture%20firm%2C%20Blueprint%20service%2C%20Building%20designer%2C%20Building%20firm%2C%20Service%20establishment%2C%20Housing%20development&extract_emails_and_contacts=true"
        
        print("Fetching business data from API...")
        conn.request("GET", query_path, headers=headers)
        
        res = conn.getresponse()
        if res.status != 200:
            print(f"API request failed with status code: {res.status}")
            return None
            
        data = json.loads(res.read().decode("utf-8"))
        print(f"Successfully fetched data for {len(data.get('data', []))} businesses")
        return data
    except Exception as e:
        print(f"Error fetching business data: {e}")
        return None

def process_business_data(data):
    """
    Process the business data to extract relevant information
    
    Args:
        data (dict): The API response data
        
    Returns:
        list: Processed business data or empty list if processing failed
    """
    try:
        data_list = data.get("data", [])
        if not data_list:
            print("No business data found in API response")
            return []
            
        print(f"Processing {len(data_list)} business records...")
        
        # Columns to exclude
        exclude_columns = [
            'google_id', 'place_id', 'google_mid', 'phone_number', 'place_link', 'cid', 'owner_id',
            'latitude', 'longitude', 'working_hours', 'owner_link', 'booking_link', 
            'reservations_link', 'photos_sample', 'reviews_link', 'reviews_per_rating', 
            'photo_count', 'order_link', 'price_level', 'street_address',
            'emails_and_contacts'  # Exclude this as we'll extract its contents
        ]
        
        # Process each item to extract email and contact information
        for item in data_list:
            # Initialize new fields
            item['emails'] = ""
            item['phone_numbers'] = ""
            item['social_media'] = {}
            
            # Extract from emails_and_contacts if it exists and is not None
            if 'emails_and_contacts' in item and item['emails_and_contacts']:
                contacts = item['emails_and_contacts']
                if isinstance(contacts, str):
                    try:
                        contacts = json.loads(contacts)
                    except json.JSONDecodeError:
                        contacts = {}
                
                # Extract emails
                if 'emails' in contacts and contacts['emails']:
                    item['emails'] = ",".join(contacts['emails'])
                
                # Extract phone numbers
                if 'phone_numbers' in contacts and contacts['phone_numbers']:
                    item['phone_numbers'] = ",".join(contacts['phone_numbers'])
                
                # Extract social media - collect all social platforms except emails and phone_numbers
                social_media = {}
                for key, value in contacts.items():
                    if key not in ['emails', 'phone_numbers'] and value:
                        social_media[key] = value
                
                # Convert social media dict to JSON string if it's not empty
                if social_media:
                    item['social_media'] = json.dumps(social_media)
        
        print(f"Successfully processed {len(data_list)} business records")
        return data_list
    except Exception as e:
        print(f"Error processing business data: {e}")
        return []

def save_to_csv(data_list, file_path):
    """
    Save the processed business data to a CSV file
    
    Args:
        data_list (list): The processed business data
        file_path (str): Path to save the CSV file
        
    Returns:
        bool: True if the data was successfully saved, False otherwise
    """
    try:
        if not data_list:
            print("No data to save")
            return False
            
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Define column order: specified columns first, then all others
        priority_fields = ['business_id', 'name', 'phone_numbers', 'emails', 'social_media']
        
        # Columns to exclude
        exclude_columns = [
            'google_id', 'place_id', 'google_mid', 'phone_number', 'place_link', 'cid', 'owner_id',
            'latitude', 'longitude', 'working_hours', 'owner_link', 'booking_link', 
            'reservations_link', 'photos_sample', 'reviews_link', 'reviews_per_rating', 
            'photo_count', 'order_link', 'price_level', 'street_address',
            'emails_and_contacts'  # Exclude this as we'll extract its contents
        ]
        
        # Get remaining fields (excluding the ones to exclude and already prioritized)
        remaining_fields = [field for field in data_list[0].keys() 
                           if field not in exclude_columns and field not in priority_fields]
        
        # Final field order
        fieldnames = priority_fields + remaining_fields
        
        print(f"Saving data to {file_path}...")
        with open(file_path, mode='w', newline='', encoding='utf-8') as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            for item in data_list:
                # Create a new dictionary without the excluded columns
                filtered_item = {key: value for key, value in item.items() if key not in exclude_columns}
                writer.writerow(filtered_item)
                
        print(f"Successfully saved {len(data_list)} records to {file_path}")
        return True
    except Exception as e:
        print(f"Error saving data to CSV: {e}")
        return False

def main():
    """
    Main function to fetch, process, and save business data
    
    Returns:
        bool: True if the process completed successfully, False otherwise
    """
    print("Starting leads generation process...")
    
    # Define the output file path from environment variable
    file_path = Path(os.environ.get('LEADS_CSV_PATH', "venv/data/leads.csv"))
    
    # Fetch business data from API
    data = fetch_business_data()
    if not data:
        print("Failed to fetch business data. Aborting.")
        return False
    
    # Process the business data
    data_list = process_business_data(data)
    if not data_list:
        print("Failed to process business data. Aborting.")
        return False
    
    # Save the processed data to CSV
    success = save_to_csv(data_list, file_path)
    if not success:
        print("Failed to save business data to CSV. Aborting.")
        return False
    
    print("Leads generation process completed successfully!")
    return True

if __name__ == "__main__":
    main()