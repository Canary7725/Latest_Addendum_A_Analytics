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
    # REVIEW:since you are searching a specific addendum file, the pattern can include the file name for this case
    # the tag choould contain  text such as "quarterly-addenda-updates" this would limit the file list with only addendum A files
    month_year_pattern = re.compile(r'\b(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{4}', re.IGNORECASE)
    # REVIEW:change this method to get all the 2025(latest year) files, if the count is greater than 2 then
    # compare the months of those files, choose the latest month from those files

    for tr in tr_blocks:
        if "Addendum A" not in tr:  # Only proceed if this row contains "Addendum A"
            continue

        # no need if you match the pattern for addendum A files

        a_match = a_tag_pattern.search(tr)
        if a_match:
            href = a_match.group(1)
            link_text = a_match.group(2)

            # Find "Month Year" in the link text
            # REVIEW: use the current date to pick up the current year and use thhe current year in the pattern match to limit the search
            # and not list all the files that have older date than the current year
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

    #REVIEW: write an alternatinve function where the html is read line by line and print the effeciency in terms of memory usage and time consumption. Use tracemalloc

    zip_path = match.group(1)
    print(f"HERE ZIP PATH {zip_path}")
    direct_url = urljoin(url, zip_path)
    print(f"HERE ZIP URL:{direct_url}")
    filename = os.path.basename(zip_path)
    print(f"HERE FILENAME:{filename}")
    urllib.request.urlretrieve(direct_url, filename)
    print(f'Downloaded at the current directory: {filename}')

    # REVIEW:write a code snippet that captures the metadata of the file, example the size and date and print the information

    with ZipFile(f'./{filename}') as zipObject:
        zipObject.extractall(path="./")
        name_without_ext = os.path.splitext(filename)[0]
        print(f"FILE NAME WITHOUT EXTENSION:{name_without_ext}")

        with ZipFile(filename) as zipObject:
            zipObject.extractall(path="./")
            extracted_files = zipObject.namelist()
            print(f"EXTRACED FILES:{extracted_files}")

            for extracted_file in extracted_files:
                original_path = os.path.join("./", extracted_file)
                print(f"ORIGINAL PATH:{original_path}")

                if os.path.isfile(original_path):
                    ext = os.path.splitext(extracted_file)[1].lower()
                    print(f"EXTENTSINO :{ext}")
                    if ext in [".csv", ".xlsx"]:
                        new_path = os.path.join("./", f"{name_without_ext}{ext}")
                        os.rename(original_path, new_path)
                    #REVIEW: if the file name mattches with the file name that is needed then breal the loop aand exit no need to iterate through all the files
    os.remove(filename)