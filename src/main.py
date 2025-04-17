from parser import batch_parse

import pandas as pd

from carousell_scraper import extract_html, extract_listings

pd.options.display.float_format = "{:.2f}".format


website_url = "https://www.carousell.ph/categories/property-102/property-house-and-lot-for-sale-868/?addRecent=true&canChangeKeyword=true&includeSuggestions=true&location_ids=174&search=salcedo&searchId=fjRYnH&t-search_query_source=direct_search"

pages_html = extract_html(website_url, city_string="Salcedo Village")
title, properties = extract_listings(pages_html)
properties = batch_parse(
    properties,
    "title",
    "Extract only the building name, know that it is in the Philippines. You need to be SURE of your response and strictly follow formats.",
    max_workers=10,
)


def replace_title(row, from_str, to_str):
    if from_str.lower() in row["building"].lower():
        row["building"] = to_str

    return row


properties["price"] = (
    properties["price"]
    .str.replace("PHP", "", regex=False)  # Remove 'PHP'
    .str.replace(",", "", regex=False)  # Remove commas
    .str.strip()  # Remove leading/trailing spaces
    .astype(float)  # Convert to integer
)

# Vectorized cleaning for size with pd.to_numeric
properties["size"] = pd.to_numeric(
    properties["size"]
    .str.replace("sqm", "", regex=False)  # Remove 'sqm'
    .str.strip(),  # Remove leading/trailing spaces
    errors="coerce",  # Convert invalid entries to NaN
).fillna(pd.NA)

properties = properties.apply(lambda row: replace_title(row, "Ellis", "Ellis"), axis=1)
properties = properties.apply(
    lambda row: replace_title(row, "Rise", "The Rise"), axis=1
)
properties = properties.apply(
    lambda row: replace_title(row, "Triomphe", "Le Triomphe"), axis=1
)
properties = properties.apply(
    lambda row: replace_title(row, "Shang Salcedo", "Shang Salcedo Place"), axis=1
)
properties = properties.apply(
    lambda row: replace_title(row, "Two Roxas", "Two Roxas"), axis=1
)


properties["per_sqm"] = properties["price"] / properties["size"]

# Group by building, get top 10 lowest per_sqm for each group, and add diff_mean_min
result = properties.sort_values("per_sqm").groupby("building").head(10).copy()

# Calculate mean and min per_sqm for each group
stats = (
    result.groupby("building")["per_sqm"]
    .agg(["mean", "min"])
    .rename(columns={"mean": "mean_per_sqm", "min": "min_per_sqm"})
)
stats["diff_mean_min"] = stats["mean_per_sqm"] - stats["min_per_sqm"]

# Merge stats back to result
result = result.merge(stats["diff_mean_min"], left_on="building", right_index=True)

print(result[["building", "per_sqm", "diff_mean_min"]])
