import pandas as pd
import selenium.webdriver as webdriver
from bs4 import BeautifulSoup
from selenium.common.exceptions import (
    ElementClickInterceptedException,
    ElementNotInteractableException,
    TimeoutException,
)
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait


def extract_html(search_result_page, city_string, max_clicks=None):
    """
    Loads a search results page, applies a city/location filter, and clicks the "Show More Results" button repeatedly to load all listings.

    Args:
        search_result_page (str): URL of the search results page.
        city_string (str): City string for location filtering.
        max_clicks (int, optional): Maximum number of times to click "Show More Results". Defaults to None (unlimited).

    Returns:
        str: HTML content after all results are loaded and filters are applied.
    """
    chrome_driver_path = "./chromedriver.exe"
    options = Options()
    driver = webdriver.Chrome(service=Service(chrome_driver_path), options=options)

    try:
        driver.get(search_result_page)
        print("Page loading...")
        _set_location_filter(driver, city_string)
        _click_search_button(driver)
        click_count = 0
        while max_clicks is None or click_count < max_clicks:
            if not _click_show_more(driver):
                break
            click_count += 1
            print(f"Clicked 'Show More' {click_count} times...")
        return driver.page_source
    finally:
        driver.quit()
        print("Browser closed")


def click_checkbox_by_label(driver, location, timeout=10):
    # Match the label that contains a span with the location text
    label_xpath = f"//label[.//span[contains(normalize-space(.), '{location}')]]"

    label = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, label_xpath))
    )

    # Click via JavaScript (in case it's partially hidden)
    driver.execute_script("arguments[0].click();", label)


def _set_location_filter(driver, location):
    """Helper to set location filter"""
    try:
        # Input location
        location_input = WebDriverWait(driver, 10).until(
            EC.visibility_of_element_located(
                (By.CSS_SELECTOR, "input[aria-label='Location']")
            )
        )
        location_input.clear()
        location_input.send_keys(location)

        click_checkbox_by_label(driver, location)

    except TimeoutException as e:
        print(f"Failed to set location filter: {str(e)}")
        raise


def _click_search_button(driver):
    """Helper to click search button"""
    try:
        # Wait for the button with the text 'Search' to be clickable
        button_xpath = "//button[./div[text()='Search']]"
        button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, button_xpath))
        )

        # Click the button
        driver.execute_script("arguments[0].click();", button)

        show_more_button_xpath = "//button[normalize-space(text())='Show more results']"

        # Wait for the "Show more results" button to be clickable
        WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, show_more_button_xpath))
        )

        print("'Show more results' button is visible and clickable. Page has loaded.")

    except TimeoutException:
        # Button not found or page didn't load
        print("Search button not found or page didn't load")


def _click_show_more(driver):
    """Helper to click the 'Show More' button with specific text"""
    try:
        show_more_button_xpath = "//button[normalize-space()='Show more results']"

        # Wait until the button is visible and clickable (wait longer: 40 seconds)
        show_more_button = WebDriverWait(driver, 40).until(
            EC.element_to_be_clickable((By.XPATH, show_more_button_xpath))
        )

        # Scroll into view to avoid click interception
        driver.execute_script(
            "arguments[0].scrollIntoView({block: 'center'});", show_more_button
        )

        # Try clicking normally first
        try:
            show_more_button.click()
        except (ElementClickInterceptedException, ElementNotInteractableException):
            print("Click intercepted or not interactable, using JS click fallback.")
            driver.execute_script("arguments[0].click();", show_more_button)

        print("'Show More' button clicked.")
        return True

    except TimeoutException:
        print("❌ Timed out waiting for 'Show More' button to be clickable.")
        return False
    except Exception as e:
        print(f"❌ Unexpected error while clicking 'Show More': {str(e)}")
        return False


def extract_listings(html):
    # Parse with Beautiful Soup using the LXML parser
    soup = BeautifulSoup(html, "lxml")
    title_tag = soup.find("title")
    page_title = title_tag.text.strip() if title_tag else ""

    def extract_from_card(listing):
        # Initialize variables
        title = price = bedrooms = bathrooms = size = location = remarks = ""
        image_url = link = seller_profile = seller_name = ""
        likes = "0"
        days_ago = ""

        # Extract title: try <p title=...> else <p> text
        title_tag = listing.find("p", {"title": True})
        if title_tag:
            title = title_tag["title"]
        else:
            # fallback: <p> with class and text
            p_tags = listing.find_all("p")
            for p in p_tags:
                if p.text and len(p.text.strip()) > 0:
                    title = p.text.strip()
                    break

        # Extract price: try <span title=...> else <span> text startswith PHP
        price_tag = listing.find(
            lambda tag: tag.name == "span" and tag.get("title", "").startswith("PHP")
        )
        if price_tag:
            price = price_tag["title"]
        else:
            span_tags = listing.find_all("span")
            for span in span_tags:
                if span.text.strip().startswith("PHP"):
                    price = span.text.strip()
                    break

        # Extract features: bedrooms, bathrooms, size
        features_div = listing.find("div", class_=lambda x: x and "D_qc" in x)
        if features_div:
            feature_spans = features_div.find_all("span", {"title": True})
            for span in feature_spans:
                title_val = span["title"]
                if "sqm" in title_val:
                    size = title_val
                elif title_val.isdigit():
                    # Check previous sibling img for bed/bath
                    prev_img = span.find_previous_sibling("img")
                    if prev_img and "ic_bed.svg" in prev_img.get("src", ""):
                        bedrooms = title_val
                    elif prev_img and "ic_bath.svg" in prev_img.get("src", ""):
                        bathrooms = title_val
                else:
                    remarks = title_val

        # Extract image URL
        img_tag = listing.find("img", {"alt": True, "src": True})
        image_url = img_tag["src"] if img_tag else ""

        # Extract listing URL
        link_tag = listing.find("a", href=True)
        link = f"https://www.carousell.ph{link_tag['href']}" if link_tag else ""

        # Extract seller profile
        seller_tag = listing.find("a", href=lambda x: x and "/u/" in x)
        seller_profile = (
            f"https://www.carousell.ph{seller_tag['href']}" if seller_tag else ""
        )

        # Extract seller name
        seller_name_tag = listing.find(
            "p", {"data-testid": "listing-card-text-seller-name"}
        )
        seller_name = seller_name_tag.text.strip() if seller_name_tag else ""

        # Extract likes
        likes_tag = listing.find("button", {"data-testid": "listing-card-btn-like"})
        likes = (
            likes_tag.find("span").text.strip()
            if likes_tag and likes_tag.find("span")
            else "0"
        )

        # Extract days ago: look for <p> with class containing D_pw or D_jG
        days_ago_tag = listing.find(
            "p", class_=lambda x: x and ("D_pw" in x or "D_jG" in x or "D_qa" in x)
        )
        days_ago = days_ago_tag.text.strip() if days_ago_tag else ""

        return {
            "title": title,
            "price": price,
            "bedrooms": bedrooms,
            "bathrooms": bathrooms,
            "size": size,
            "location": location,
            "remarks": remarks,
            "image_url": image_url,
            "listing_url": link,
            "seller_profile": seller_profile,
            "seller_name": seller_name,
            "likes": likes,
            "days_ago": days_ago,
        }

    # Extract Listings
    listings = []

    # Find all listing cards using data-testid pattern
    for listing in soup.find_all(
        "div", {"data-testid": lambda x: x and x.startswith("listing-card-")}
    ):
        listings.append(extract_from_card(listing))

    if not listings:
        raise ValueError("No listings were found")

    # Convert to DataFrame
    listings = pd.DataFrame(listings)
    return page_title, listings


def main():
    website_url = "https://www.carousell.ph/categories/property-102/property-house-and-lot-for-sale-868/?addRecent=true&canChangeKeyword=true&includeSuggestions=true&searchId=-cgH4i&t-search_query_source=direct_search"

    pages_html = extract_html(website_url, city_string="Salcedo Village", max_clicks=4)
    title, properties = extract_listings(pages_html)

    return title, properties


if __name__ == "__main__":
    title, properties = main()
