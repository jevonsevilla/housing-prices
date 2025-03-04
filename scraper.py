import json
import logging
import re
import time
from typing import Dict, List

import pandas as pd
import requests
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_fixed


class PhilippinesRealEstateScraper:
    def __init__(self):
        # Configure logging
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(__name__)

        # Headers to mimic browser requests
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }

    def clean_price(self, price_str: str) -> float:
        """
        Clean and convert price string to float

        Args:
            price_str (str): Raw price string

        Returns:
            float: Cleaned numeric price
        """
        try:
            # Remove currency symbols, commas, and non-numeric characters
            cleaned = re.sub(r"[^\d.]", "", str(price_str))
            return float(cleaned) if cleaned else 0.0
        except (ValueError, TypeError):
            self.logger.warning(f"Could not clean price: {price_str}")
            return 0.0

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
    def fetch_page(self, url: str) -> requests.Response:
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response

    def scrape_lamudi(self, max_pages: int = 5) -> List[Dict]:
        """
        Scrape real estate listings from Lamudi Philippines

        Args:
            max_pages (int): Maximum number of pages to scrape

        Returns:
            List[Dict]: Extracted property listings
        """
        listings = []
        base_url = "https://www.lamudi.com.ph/rent/"

        for page in range(1, max_pages + 1):
            try:
                url = f"{base_url}?page={page}"
                response = self.fetch_page(url)

                soup = BeautifulSoup(response.content, "html.parser")

                # Find all property listing cards
                property_cards = soup.find_all("div", class_="listing-card")

                for card in property_cards:
                    try:
                        # Extract listing details
                        title = card.find("h2", class_="listing-title").text.strip()
                        price_elem = card.find("div", class_="listing-price")
                        price = price_elem.text.strip() if price_elem else "N/A"

                        location_elem = card.find("div", class_="listing-location")
                        location = (
                            location_elem.text.strip() if location_elem else "Unknown"
                        )

                        listings.append(
                            {
                                "title": title,
                                "price_text": price,
                                "location": location,
                                "source": "Lamudi",
                            }
                        )
                    except Exception as card_error:
                        self.logger.warning(f"Error parsing card: {card_error}")

                # Polite scraping
                time.sleep(1)

            except requests.RequestException as e:
                self.logger.error(f"Error scraping Lamudi page {page}: {e}")
                break

        return listings

    def scrape_property24(self, max_pages: int = 5) -> List[Dict]:
        """
        Scrape real estate listings from Property24 Philippines

        Args:
            max_pages (int): Maximum number of pages to scrape

        Returns:
            List[Dict]: Extracted property listings
        """
        listings = []
        base_url = "https://www.property24.ph/for-rent"

        for page in range(1, max_pages + 1):
            try:
                url = f"{base_url}?page={page}"
                response = self.fetch_page(url)

                soup = BeautifulSoup(response.content, "html.parser")

                # Find all property listing cards
                property_cards = soup.find_all("div", class_="property-card")

                for card in property_cards:
                    try:
                        # Extract listing details
                        title_elem = card.find("h3", class_="property-title")
                        title = title_elem.text.strip() if title_elem else "N/A"

                        price_elem = card.find("div", class_="property-price")
                        price = price_elem.text.strip() if price_elem else "N/A"

                        location_elem = card.find("div", class_="property-location")
                        location = (
                            location_elem.text.strip() if location_elem else "Unknown"
                        )

                        listings.append(
                            {
                                "title": title,
                                "price_text": price,
                                "location": location,
                                "source": "Property24",
                            }
                        )
                    except Exception as card_error:
                        self.logger.warning(f"Error parsing card: {card_error}")

                # Polite scraping
                time.sleep(1)

            except requests.RequestException as e:
                self.logger.error(f"Error scraping Property24 page {page}: {e}")
                break

        return listings

    def save_to_csv(
        self, listings: List[Dict], filename: str = "real_estate_listings.csv"
    ):
        """
        Save listings to CSV with robust error handling

        Args:
            listings (List[Dict]): List of property listings
            filename (str): Output CSV filename
        """
        try:
            # Create DataFrame
            df = pd.DataFrame(listings)

            # Add cleaned price column
            df["cleaned_price"] = df["price_text"].apply(self.clean_price)

            # Save to CSV
            df.to_csv(filename, index=False, encoding="utf-8")
            self.logger.info(f"Saved {len(listings)} listings to {filename}")
        except Exception as e:
            self.logger.error(f"Error saving to CSV: {e}")

    def aggregate_listings(self, listings: List[Dict]) -> Dict:
        """
        Aggregate listing data with robust error handling

        Args:
            listings (List[Dict]): List of property listings

        Returns:
            Dict: Aggregated listing statistics
        """
        try:
            # Create DataFrame
            df = pd.DataFrame(listings)

            # Clean prices
            df["cleaned_price"] = df["price_text"].apply(self.clean_price)

            # Basic aggregations
            aggregations = {
                "total_listings": len(listings),
                "average_price": float(df["cleaned_price"].mean()),
                "median_price": float(df["cleaned_price"].median()),
                "min_price": float(df["cleaned_price"].min()),
                "max_price": float(df["cleaned_price"].max()),
                "listings_by_source": df["source"].value_counts().to_dict(),
                "price_by_location": df.groupby("location")["cleaned_price"]
                .mean()
                .to_dict(),
            }

            return aggregations
        except Exception as e:
            self.logger.error(f"Error aggregating listings: {e}")
            return {"total_listings": 0, "error": str(e)}


def main():
    # Initialize extractor
    extractor = PhilippinesRealEstateScraper()

    # Collect listings from multiple sources
    all_listings = []

    # Uncomment and implement actual scraping (with caution and website permission)
    try:
        # Scrape Lamudi listings
        lamudi_listings = extractor.scrape_lamudi(max_pages=3)
        all_listings.extend(lamudi_listings)

        # Scrape Property24 listings
        property24_listings = extractor.scrape_property24(max_pages=3)
        all_listings.extend(property24_listings)

        extractor.logger.info(all_listings)
    except Exception as e:
        extractor.logger.error(f"Error during scraping: {e}")

    # Save to CSV
    extractor.save_to_csv(all_listings)

    # Generate aggregations
    try:
        aggregations = extractor.aggregate_listings(all_listings)
        extractor.logger.info("\nListing Aggregations:")
        extractor.logger.info(json.dumps(aggregations, indent=2))
    except Exception as e:
        extractor.logger.error(f"Error in aggregating listings: {e}")


if __name__ == "__main__":
    main()
