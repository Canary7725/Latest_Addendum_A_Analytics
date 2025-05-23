from email.utils import parsedate_to_datetime
import os
import tracemalloc
import urllib.request
import re
from datetime import datetime
import time
from urllib.parse import urljoin
from zipfile import ZipFile
import urllib.request
import os
import re


def get_latest_addendum_a_href(url):
    current_year = datetime.now().year
    
    months = ("january|february|march|april|may|june|july|august|"
              "september|october|november|december")
    month_year_pattern = re.compile(rf'\b({months})\s+{current_year}', re.IGNORECASE)

    a_tag_pattern = re.compile(
        rf'<a[^>]+href=["\']([^"\']*quarterly-addenda-updates/[^"\']*\b'
        rf'(?:{months})-{current_year}(?:-[^"\'>]*)*-addendum(?:-a(?:-[a-z])*)?)["\'][^>]*>'
        r'(.*?)</a>',
        re.IGNORECASE
    )


    matched_links = []

    with urllib.request.urlopen(url) as response:
        for line in response:
            try:
                line = line.decode('utf-8')
            except UnicodeDecodeError:
                continue

            match = a_tag_pattern.search(line)
            if match:
                href = match.group(1)
                link_text = match.group(2)
                full_url = urljoin(url, href)
                matched_links.append((link_text, href, full_url))


    if not matched_links:
        print("Latest URL not found")
        return None

    if len(matched_links) == 1:
        return matched_links[0][2]

    month_map = {m: i for i, m in enumerate([
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december"], start=1)}

    latest_url = None
    latest_month = -1

    for link_text, href, full_url in matched_links:
        date_match = month_year_pattern.search(link_text)
        if date_match:
            month_str = date_match.group(1).lower()
        else:
            href_match = re.search(rf'\b({months})-{current_year}', href, re.IGNORECASE)
            if href_match:
                month_str = href_match.group(1).lower()
            else:
                continue

        month_val = month_map.get(month_str, 0)
        if month_val > latest_month:
            latest_month = month_val
            latest_url = full_url
    return latest_url


def get_file_metadata(file_url):
    request = urllib.request.Request(file_url, method='HEAD')
    with urllib.request.urlopen(request) as response:
        headers = response.info()
        content_length = headers.get("Content-Length")
        last_modified = headers.get("Last-Modified")

        print(f"File URL: {file_url}")
        print(f"File Size: {int(content_length) / 1024:.2f} KB" if content_length else "File Size: Unknown")

        if last_modified:
            last_modified_dt = parsedate_to_datetime(last_modified)
            print(f"Last Modified: {last_modified_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print("Last Modified: Unknown")

def download_file_from_url_and_extract(url,use_xlsx):
    latest_href = get_latest_addendum_a_href(url)

    tracemalloc.start()
    start_time = time.time()

    zip_path = None
    with urllib.request.urlopen(latest_href) as response:
        for line in response:
            try:
                line = line.decode("utf-8")
            except UnicodeDecodeError:
                continue
            pattern=re.compile(r'file=(/files/zip/[^"\']+\.zip)',re.IGNORECASE)
            match = pattern.search(line)
            if match:
                zip_path = match.group(1)
                print(zip_path)
                break
    end_time = time.time()
    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    print(f"Memory usage (Current: {current / 1024:.2f} KB, Peak: {peak / 1024:.2f} KB)")
    print(f"Execution time: {end_time - start_time:.4f} seconds")

    assert zip_path is not None, "No ZIP path found in the HTML."

    print(f"HERE ZIP PATH {zip_path}")
    direct_url = urljoin(url, zip_path)
    print(f"HERE ZIP URL: {direct_url}")
    filename = os.path.basename(zip_path)
    print(f"HERE FILENAME: {filename}")

    urllib.request.urlretrieve(direct_url, filename)
    print(f"Downloaded at the current directory: {filename}")

    # Print file metadata
    get_file_metadata(direct_url)

    # Extract and rename target file
    with ZipFile(filename) as zipObject:
        zipObject.extractall(path="./")
        name_without_ext = os.path.splitext(filename)[0]
        print(f"FILE NAME WITHOUT EXTENSION: {name_without_ext}")

        extracted_files = zipObject.namelist()
        print(f"EXTRACTED FILES: {extracted_files}")

        for extracted_file in extracted_files:
            original_path = os.path.join("./", extracted_file)
            print(f"ORIGINAL PATH: {original_path}")

            if os.path.isfile(original_path):
                ext = os.path.splitext(extracted_file)[1].lower()
                print(f"EXTENSION: {ext}")
                if use_xlsx:
                    if ext==".xlsx":
                        new_path=os.path.join("./",f"{name_without_ext}{ext}")
                        os.rename(original_path, new_path)
                    elif ext == ".csv":
                        os.remove(original_path)
                else:
                    if ext==".csv":
                        new_path=os.path.join("./",f"{name_without_ext}{ext}")
                        os.rename(original_path,new_path)
                    else:
                        os.remove(original_path)

    os.remove(filename)
