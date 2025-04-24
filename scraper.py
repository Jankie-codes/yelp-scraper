from parsel import Selector
from scrapfly import ScrapeConfig, ScrapflyClient, ScrapeApiResponse
from dotenv import load_dotenv
import os
import json
import urllib.parse
import pandas as pd
import sys

load_dotenv()

api_key_test = os.getenv("API_KEY_TEST")
api_key_production = os.getenv("API_KEY_PRODUCTION")

DEBUG = False

API_KEY = api_key_production if not DEBUG else api_key_test

def scrape_yelp(business_desc, location, start):
    scrapfly = ScrapflyClient(key=API_KEY)
    print(f"Scraping the following url: https://www.yelp.ca/search?find_desc={urllib.parse.quote(business_desc)}&find_loc={urllib.parse.quote(location)}&start={start}")
    result: ScrapeApiResponse = scrapfly.scrape(ScrapeConfig(
        tags=[
        "player","project:default"
        ],
        asp=True,
        url=f"https://www.yelp.ca/search?find_desc={urllib.parse.quote(business_desc)}&find_loc={urllib.parse.quote(location)}&start={start}"
    ))
    print(f"Successfully scraped yelp QUERY: {business_desc} at LOCATION: {location} results #{start} to {start+10}")
    return result.content

def parse_yelp_search_results_output(html: str):
    """parse listing data from the search XHR data"""
    search_data = []
    selector = Selector(text=html)
    script = selector.xpath("//script[@data-id='react-root-props']/text()").get()
    data = json.loads(script.split("react_root_props = ")[-1].rsplit(";", 1)[0])
    for item in data["legacyProps"]["searchAppProps"]["searchPageProps"]["mainContentComponentsListProps"]:
        # filter search data cards
        if "bizId" in item.keys():
            search_data.append(item)
        # filter the max results count
        elif "totalResults" in item["props"]:
            total_results = item["props"]["totalResults"]
    #print(json.dumps(search_data, indent=2))
    #print("results", total_results)
    return {"search_data": search_data, "total_results": total_results}

def clean_url(url: str) -> str:
    # Remove "https://", "http://", and "www."
    url = url.replace("https://", "").replace("http://", "").replace("www.", "")
    return url

def remove_affiliate_component(input_url: str) -> str:
    """
    This function extracts the actual destination URL by removing the affiliate 
    redirect and decoding the URL components.
    
    Args:
    - input_url: The URL containing the redirect and affiliate parameters.
    
    Returns:
    - str: The final destination URL after cleaning out the affiliate components.
    """
    # Extract the 'redirect_url' parameter
    parsed_url = urllib.parse.urlparse(input_url)
    query_params = urllib.parse.parse_qs(parsed_url.query)

    # Get the URL inside the 'redirect_url' parameter
    redirect_url = query_params.get('redirect_url', [None])[0]
    
    if redirect_url:
        # Decode the redirect_url (first level decoding)
        decoded_redirect_url = urllib.parse.unquote(redirect_url)
        
        # Extract the inner URL from 'url' parameter inside the redirect_url
        inner_query_params = urllib.parse.parse_qs(urllib.parse.urlparse(decoded_redirect_url).query)
        inner_url = inner_query_params.get('url', [None])[0]

        if inner_url:
            # Decode the inner URL (second level decoding)
            final_url = urllib.parse.unquote(inner_url)
            return final_url
        else:
            return "No inner URL found"
    else:
        return input_url

def make_csv_rows(search_data):
    rows = []

    for biz_obj in search_data:
        transformed_row = {
            "bizId": biz_obj["bizId"],
            "name": biz_obj["searchResultBusiness"]["name"],
            "categories": [category["title"] for category in biz_obj["searchResultBusiness"]["categories"]],
            "phone number": biz_obj["searchResultBusiness"]["phone"],
            "website": clean_url(remove_affiliate_component(biz_obj["searchResultBusiness"]["website"]["href"])) if biz_obj["searchResultBusiness"]["website"] else None,
            "rating": biz_obj["searchResultBusiness"]["rating"],
            "reviewCount": biz_obj["searchResultBusiness"]["reviewCount"],
            "yelp_url": f"https://www.yelp.ca/biz/{biz_obj["searchResultBusiness"]["alias"]}"
        }
        rows.append(transformed_row)
    #print("ROWS", json.dumps(rows, indent=2))
    return rows

def append_to_csv(rows, csv_filename):
    # Convert rows into a pandas DataFrame
    df_new = pd.DataFrame(rows)
    
    # Check if the CSV file exists
    try:
        # Read the existing CSV into a DataFrame
        df_existing = pd.read_csv(csv_filename)
        
        # Concatenate the new rows to the existing data
        df_combined = pd.concat([df_existing, df_new])
        
        # Remove duplicates based on the 'bizId' column (you can change the column if needed)
        df_combined = df_combined.drop_duplicates(subset=["bizId"], keep="first")
    
    except FileNotFoundError:
        # If the file doesn't exist, just use the new rows
        df_combined = df_new
    
    # Write the combined data back to the CSV
    df_combined.to_csv(csv_filename, index=False)

    #print(f"Rows successfully appended and duplicates removed in {csv_filename}.")

def get_confirmation(prompt, default='n'):
    # Prepare the prompt with the default value in uppercase and the other option in lowercase
    prompt_message = f"{prompt} [{'Y/n' if default == 'y' else 'y/N'}] "

    while True:
        # Prompt the user for confirmation, including the default value in uppercase
        user_input = input(prompt_message).strip().lower()
        
        # If user presses Enter, use the default value
        if user_input == '':
            user_input = default
        
        if user_input == 'y':
            return True  # User confirmed with 'y'
        elif user_input == 'n':
            return False  # User denied with 'n'
        else:
            print("Invalid input. Please enter 'y' or 'n'.")

def main():
    print("=============THIS IS THE YELP SCRAPER============")
    print("Format: python scraper.py [search query] [location query] [output csv name/directory]")
    args = sys.argv[1:]
    if len(args) < 3:
        print("ERROR: invalid inputs. Exiting...")   
        sys.exit(1)
    BUSINESS_DESC = args[0]
    LOCATION = args[1]
    CSV_OUTPUT = args[2]

    print(f"Scraping from yelp: [{BUSINESS_DESC}] in location [{LOCATION}], to be saved in csv [{CSV_OUTPUT}].")
    print(f"Debug mode is {"ON." if DEBUG else "OFF."}")
    print(f"File {CSV_OUTPUT} {"ALREADY EXISTS. Appending to EXISTING csv!" if os.path.exists(CSV_OUTPUT) else "Does not exist yet."}")
    if not get_confirmation("Please confirm: would you like to proceed?", 'n'):
        print("Confirm failed, exiting...")
        sys.exit(1)
    else:
        print("Proceeding...")

    if DEBUG:
        with open('test_yelp_output.txt', 'r') as file:
        # Read the entire content of the file
            content = file.read()

    # Print the content
    if DEBUG:
        results = parse_yelp_search_results_output(content)
    else:
        results = parse_yelp_search_results_output(scrape_yelp(BUSINESS_DESC, LOCATION, 0))
    rows = make_csv_rows(results["search_data"])
    append_to_csv(rows, CSV_OUTPUT)
    print("TOTAL RESULTS to scrape: ", results["total_results"])

    if not DEBUG:
        for i in range(10,results["total_results"], 10):
            results = parse_yelp_search_results_output(scrape_yelp(BUSINESS_DESC, LOCATION, i))
            rows = make_csv_rows(results["search_data"])
            append_to_csv(rows, CSV_OUTPUT)
    print(f"Successfully scraped all {results["total_results"]} results into csv {CSV_OUTPUT}.")
    sys.exit(0)

if __name__ == "__main__":
    main()

