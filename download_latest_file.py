import os
import urllib.request
import re
from datetime import datetime
from urllib.parse import urljoin
from zipfile import ZipFile
import urllib.request
import os
import re

def get_latest_addendum_a_href(url):
    with urllib.request.urlopen(url) as response:
        html = response.read().decode("utf-8")

    tr_pattern = re.compile(r'<tr[^>]*>.*?</tr>', re.DOTALL | re.IGNORECASE)  # Find all <tr> blocks
    tr_blocks = tr_pattern.findall(html)

    latest_date = None
    latest_href = None

    # Patterns for extracting date and href
    a_tag_pattern = re.compile(r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>', re.IGNORECASE)
    month_year_pattern = re.compile(r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}', re.IGNORECASE)

    for tr in tr_blocks:
        if "Addendum A" not in tr:  # Only proceed if this row contains "Addendum A"
            continue

        a_match = a_tag_pattern.search(tr)
        if a_match:
            href = a_match.group(1)
            link_text = a_match.group(2)

            # Find "Month Year" in the link text
            date_match = month_year_pattern.search(link_text)
            if date_match:
                try:
                    date_obj = datetime.strptime(date_match.group(0), "%B %Y")
                except ValueError:
                    continue

                if latest_date is None or date_obj > latest_date:
                    latest_date = date_obj
                    latest_href = urljoin(url, href)

    return latest_href


def download_file_from_url_and_extract(url):
    latest_href = get_latest_addendum_a_href(url)

    with urllib.request.urlopen(latest_href) as response:
        html = response.read().decode("utf-8")

    pattern = re.compile(r'file=(/files/zip/[^"\']+\.zip)', re.IGNORECASE)
    match = pattern.search(html)

    assert match is not None, "No ZIP path found in the HTML."

    zip_path = match.group(1)
    direct_url = urljoin(url, zip_path)
    filename = os.path.basename(zip_path)
    urllib.request.urlretrieve(direct_url, filename)
    print(f'Downloaded at the current directory: {filename}')

    with ZipFile(f'./{filename}') as zipObject:
        zipObject.extractall(path="./")
        name_without_ext = os.path.splitext(filename)[0]

        with ZipFile(filename) as zipObject:
            zipObject.extractall(path="./")
            extracted_files = zipObject.namelist()

            for extracted_file in extracted_files:
                original_path = os.path.join("./", extracted_file)

                if os.path.isfile(original_path):
                    ext = os.path.splitext(extracted_file)[1].lower()
                    if ext in [".csv", ".xlsx"]:
                        new_path = os.path.join("./", f"{name_without_ext}{ext}")
                        os.rename(original_path, new_path)
    os.remove(filename)