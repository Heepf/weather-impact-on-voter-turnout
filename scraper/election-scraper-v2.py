from playwright.sync_api import sync_playwright 
from playwright.sync_api import Locator, BrowserContext
from urllib.parse import urljoin
from dataclasses import dataclass
import pandas as pd
import numpy as np
import time, re, os, json

PAGE_TIMEOUT_MS = 5500
PAUSE_TIME = 1.8
   
@dataclass
class ScraperConfig:

    # ===== XPATH SELECTORS =====

    page_row_path: str

    row_href_path: str
    row_name_path: str
    row_teryt_path: str
    row_turnout_path: str

    row_attribute_path: str

    final_row_to_turnout_path: str

    # ===== METADATA =====

    turnout_snapshots: list
    election_type: str
    output_file_name: str

    # ===== FILTERS =====

    mpp_if_contains: list[str]
    ignore_row_if_contains: list[str]
    is_url_child_of_rows: bool
    gather_teryt_from_attribute: bool

    # ===== ATTRIBUTES =====

    url_attribute: str
    teryt_attribute: str

    # ===== MPP DETECTION =====

    mpps_have_no_subpage: bool
    gather_names_as_teryt: bool


def run_scraper(playwright, config: ScraperConfig):

    for i, snapshot in enumerate(config.turnout_snapshots):

        root_url = snapshot["url"]
        date = snapshot["date"]
        round = snapshot['round']

        total_teryt_codes = []
        total_turnout = []

        province_urls = []

        browser = playwright.chromium.launch()
        context = browser.new_context()
        main_page = context.new_page()

        if('21:00:00' in date):
            is_final_turnout = True
        else:
            is_final_turnout = False

        print("Data scraper initialized!")

        # ======== CENTRAL LEVEL, PROVINCE VIEW ========
        
        main_page.goto(root_url, timeout = PAGE_TIMEOUT_MS)
        time.sleep(PAUSE_TIME)
        
        # Find rows on the main page
        province_url_loc = main_page.locator(ensure_xpath(f'{config.page_row_path}{config.row_href_path}'))   
        province_url_loc = validate_all_rows(province_url_loc, config.ignore_row_if_contains)

        # Save links to the province pages
        for url in province_url_loc.all():
            href_value = url.get_attribute(config.url_attribute)
            full_url = urljoin(root_url, href_value)
            province_urls.append(full_url) 

        print(f'Found {len(province_urls)} provinces/electoral districts')

        # ======= PROVINCE LEVEL - MPP CLASSIFICATION =======
        for j, prov_url in enumerate(province_urls):
            
            main_page.goto(prov_url, timeout = PAGE_TIMEOUT_MS)
            time.sleep(PAUSE_TIME)

            # Find rows on the province pages
            row_locator = main_page.locator(ensure_xpath(config.page_row_path))
            row_locator = validate_all_rows(row_locator, config.ignore_row_if_contains)
            
            # Iterate through each administrative unit (commune level)
            for k_index, prov_row in enumerate(row_locator.all()):
                is_mpp = is_row_mpp(prov_row)
                print(f'Is MPP? {is_mpp}')

                # Select scraping strategy based off of unit type
                if is_mpp:
                    teryt, turnout = scrape_mpp(prov_row, is_final_turnout)

                else:
                    href_loc = prov_row.locator(ensure_xpath(f'{config.row_href_path}'))
                    href_value = scrape_url(href_loc,prov_row, config)
                    county_link = urljoin(prov_url, href_value) 

                    teryt, turnout = scrape_normally(context, county_link, is_final_turnout)
                
                # Fallback: Generate unique ID from names if TERYT codes are unavailable
                # The code below does not scrape; it cleanes the provided ID by the code above
                if config.gather_names_as_teryt:
                    prov_name = prov_row.locator(ensure_xpath(f'{config.row_name_path}')).inner_text()

                    # We need to take into account that multiple communes can have same names.
                    # Dfferentiate them by adding the county's name to the ID.
                    # Assumption: Urban row precedes the rural row for the same location name.

                    teryt = [f'{prov_name};gm. {string}' for string in teryt]

                    for x in range(len(teryt)):
                        
                        if "Warszawa" in teryt[x] or "warszawa" in teryt[x]:
                            teryt[x] = teryt[x].replace('gm. ', '')
                            continue

                        if len(teryt) == 1:
                            teryt[x] = teryt[x].replace('gm. ', 'm. ') 
                            continue

                        if x == 0:
                            continue

                        else:
                            if teryt[x] == teryt[x-1]:
                                teryt[x-1] = teryt[x-1].replace('gm. ', 'm. ')             

                total_teryt_codes.extend(teryt)
                total_turnout.extend(turnout)
                
                print(f'------ County {k_index + 1} out of {row_locator.count()} --- Province {j + 1} out of {len(province_urls)} --- Date: {i + 1} out of {len(config.turnout_snapshots)} ({date}) -------- Province URL: {prov_url}\nTERYT: {teryt}\nTURNOUT: {turnout}\n\n')
            

        # ======== DATA SAVING ========

        df_turnout = pd.DataFrame()
        df_turnout['TERYT'] = total_teryt_codes
        df_turnout['Date'] = date
        df_turnout['Election_type'] = config.election_type
        df_turnout['Turnout'] = total_turnout

        if round is not None:
            df_turnout['Round'] = round
        else:
            df_turnout['Round'] = np.nan
 
        file_path = f'{config.output_file_name}.csv'

        if(os.path.exists(file_path)):
            df_turnout.to_csv(file_path, mode='a', index=False, header=False)
        else:
            df_turnout.to_csv(file_path, index = False, encoding= 'utf-8', header = True)    


        browser.close()


def scrape_normally(context: BrowserContext, link_to_county:str, is_final_turnout:bool) -> tuple[list[str], list[float]]:

    teryt_list = []
    turnout_list = []

    county_page = context.new_page()
    county_page.goto(link_to_county, timeout = PAGE_TIMEOUT_MS) 
    time.sleep(PAUSE_TIME)

    rows_loc = county_page.locator(ensure_xpath(f'{config.page_row_path}'))
    rows_loc = validate_all_rows(rows_loc, config.ignore_row_if_contains)

    for c_row in rows_loc.all():
        turnout_list.append(scrape_turnout(c_row, is_final_turnout))
        teryt_list.append(scrape_teryt(c_row))
    
    county_page.close()

    return teryt_list, turnout_list


def scrape_mpp(row:Locator, is_final_turnout:bool) -> tuple[list[str], list[float]]:
    
    # MPP has no communes inside it.
    # Thats why it has only one [TERTYT, TURNOUT] pair 

    turnout = scrape_turnout(row, is_final_turnout)
    teryt = scrape_teryt(row)

    return [teryt], [turnout]


def scrape_url(links_locator:Locator, prov_row: Locator, config: ScraperConfig) -> str:

    href_value = None

    # On some websites, URLs are located outside the main table. The logic below handles that.
    # It only works if TERYT is an attribute, because those the TERYT table and the URL table are often sorted in a different way.
    if not config.is_url_child_of_rows and config.gather_teryt_from_attribute:

        main_teryt_attr = prov_row.locator(ensure_xpath(config.row_teryt_path)).get_attribute(config.teryt_attribute)

        for i in range(links_locator.count()):
            link_loc = links_locator.nth(i)
            link_loc_teryt = link_loc.get_attribute(config.teryt_attribute)

            if main_teryt_attr == link_loc_teryt:
                href_value = links_locator.nth(i).get_attribute(config.url_attribute)
                break

    # Scrape the url directly from the row if the table is using the standard layout
    else: 
        href_value = links_locator.get_attribute(config.url_attribute)

    if href_value is None:
        raise ValueError(f"Href attribute is missing, {links_locator}")
    
    return href_value


def scrape_turnout(row: Locator, is_final_turnout:bool) -> float:

    #The XPath to the turnout data sometimes differs on the 'final tunrout' page.
    if (config.final_row_to_turnout_path and is_final_turnout):
        raw_content = row.locator(ensure_xpath(f'{config.final_row_to_turnout_path}')).inner_text()

    #Scrape normally from a table.
    else:
        raw_content = row.locator(ensure_xpath(f'{config.row_turnout_path}')).inner_text()
    
    
    turnout_val = re.search(r'(\d{1,2}[,.]\d{1,2})', raw_content).group(1) # type: ignore
    turnout_val = float(turnout_val.replace(',','.'))
    return turnout_val


def scrape_teryt(row: Locator) -> str:

    # Scrape teryt directy from DOM using an attribute.
    # Assumption: TERYT codes for MPPs end with two zeros. We have to change the last digit to 1.
    if config.gather_teryt_from_attribute:

        teryt_val = row.locator(ensure_xpath(f'{config.row_teryt_path}')).get_attribute(config.teryt_attribute)

        if teryt_val is None:
            raise ValueError(f"Teryt attribute is missing at {row}")
    
        if len(teryt_val) % 2 != 0:
            teryt_val = "0" + teryt_val
        
        if teryt_val[-2:] == "00":
            teryt_val = teryt_val[:-1] + "1"
            
    #Scrape teryt from a table
    else:    
        teryt_val = row.locator(ensure_xpath(f'{config.row_teryt_path}')).inner_text()

    if teryt_val is None:
        raise ValueError(f"Teryt attribute is missing at {row}")

    return teryt_val


def is_row_mpp (row:Locator) -> bool:
    
    row_text = row.inner_text()

    # Warsaw is the only MPP that can be normally scraped (its subpage contains districts)
    if "Warszawa" in row_text or "warszawa" in row_text:
         return False 
      
    # Classify the row by its name
    for string in config.mpp_if_contains:
        if (string in row_text):
            return True

    # Classify row by link's existence
    if config.mpps_have_no_subpage and row.locator(ensure_xpath(f'{config.row_href_path}')).count() == 0:
        return True
        
    #Classify by the TERYT code. Only possible if teryt is in an attribute.
    #It looks for the last digits of the string inside the attribute (which usually contains the TERYT number)
    #If the third digit of said number is 6 or 7 classify the row as the MPP.
    if config.gather_teryt_from_attribute:
        teryt = row.locator(ensure_xpath(config.row_attribute_path)).get_attribute(config.teryt_attribute)

        if teryt is None:
            raise ValueError(f"Teryt attribute is missing, {row}")
        else:
            match = re.search(r"(\d+)\D*$", teryt)

            if match:
                teryt = match.group(1)
                if len(teryt) % 2 != 0:
                    teryt = "0" + teryt
                if teryt[2] == '6' or teryt[2] == '7':
                    return True
                else:
                    return False
            
            else:
                raise ValueError(f"Teryt not found in the attribute, {row}")
    return False


def validate_all_rows(to_validate: Locator, ignore_row_if_contains:list[str]) -> Locator:

    start_time = time.time()
    timeout_sec = PAGE_TIMEOUT_MS / 1000
    first_visible = None

    # Wait for the visibility of one of the elements
    while time.time() - start_time < timeout_sec:
        count = to_validate.count()
        for i in range(count):
            row = to_validate.nth(i)

            if row.is_visible():
                first_visible = row
                break

        if first_visible:
            break

        time.sleep(0.2)

    # Filter by visibility and text content
    # This is potentially dangerous, as this line filters paths, in which ALL children are not visible.
    # However, in the only case this line is needed, said requirement is met.
    to_validate = to_validate.filter(visible = True)

    for string in ignore_row_if_contains:
        to_validate = to_validate.filter(has_not_text=string)

    return to_validate


# Ensure the xpath has an 'xpath=' at the beginning.
def ensure_xpath(path:str) -> str:
    if not path.startswith('xpath='):
        return f'xpath={path}'
    else:
        return path
    


def load_config(config_key: str):

    with open('turnout_scraper_config.json', 'r', encoding='utf-8') as f:
        data = json.load(f)[config_key]  

        return ScraperConfig(

                # ===== XPATH SELECTORS =====
                page_row_path= data["PAGE_TO_ROW_XPATH"],  # A Locator path from root to the each of the rows.
                row_href_path= data["ROW_TO_HREF_XPATH"], # A Locator path from each of the rows to the link.
                row_teryt_path = data["ROW_TO_TERYT_XPATH"], # A Locator path from each of the rows to TERYT.
                row_name_path = data["ROW_TO_NAME_XPATH"], # A Locator path from each of the rows to the county's/commune's name.
                row_turnout_path = data['ROW_TO_TURNOUT_XPATH'], # A Locator path from each of the rows to the turnout.

                final_row_to_turnout_path= data["FINAL_ROW_TO_TURNOUT_XPATH"], # Sometimes the path from row to turnout changes for the final turnout report.

                row_attribute_path= data["ROW_ATTRIBUTE_PATH"], #If gather_teryt_from_attribute is set to True, this is variable to modify the attribute's path.
                
                # ===== METADATA =====
                turnout_snapshots= data["URLS_CONTEXT"], #Dictionary definitions.
                election_type= data["ELECTION_TYPE"], 
                output_file_name= data["OUTPUT_FILE_NAME"], 

                # ===== FILTERS =====
                mpp_if_contains= data["MPP_IF_CONTAINS"], #If a row contains a certain string, it is an MPP.
                ignore_row_if_contains= data["IGNORE_ROW_IF_CONTAINS"], #If a row contains a certain string, throw it out.
                is_url_child_of_rows= data["IS_URL_CHILD_OF_ROWS"], #If turnout data and links are in the same table.
                gather_teryt_from_attribute= data["GATHER_TERYT_FROM_ATTRIBUTE"], #If we are forced to gather TERYT from attributes (e. g. data-id), or links themselves.

                # ===== ATTRIBUTES =====
                url_attribute= data["URL_ATTRIBUTE"], #Attribute which contains the links (eg. href)
                teryt_attribute= data["TERYT_ATTRIBUTE"], #Attribute that contains TERYT (e.g. data-id)

                # ===== MPP DETECTION =====
                mpps_have_no_subpage = data["MPPS_HAVE_NO_SUBPAGE"], #If mpp have no subpages whatsoever, then it is True.
                gather_names_as_teryt = data["GATHER_NAMES_AS_TERYT"], #Fallback: Use commune and province names as IDs when TERYT is unavailable.
        )
    
with sync_playwright() as playwright:

    # Specify which config from the turnout_scraper_config.json to run
    config = load_config("2005_PREZ")
    run_scraper(playwright, config)



