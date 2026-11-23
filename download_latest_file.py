import os
import re
import urllib.request
from datetime import datetime
from urllib.parse import urljoin
from zipfile import ZipFile


def get_latest_addendum_a_href(url):
    with urllib.request.urlopen(url, timeout=30) as response:
        html = response.read().decode("utf-8")

    row_pattern = re.compile(r"<tr[^>]*>.*?</tr>", re.DOTALL | re.IGNORECASE)
    link_pattern = re.compile(
        r'<a[^>]+href=["\']([^"\']+)["\'][^>]*>(.*?)</a>',
        re.IGNORECASE,
    )
    date_pattern = re.compile(
        r"\b(january|february|march|april|may|june|july|august|"
        r"september|october|november|december)\s+\d{4}",
        re.IGNORECASE,
    )

    latest_date = None
    latest_href = None

    for row in row_pattern.findall(html):
        if "addendum a" not in row.lower():
            continue

        link_match = link_pattern.search(row)
        if link_match is None:
            continue

        href, link_text = link_match.groups()
        date_match = date_pattern.search(link_text)
        if date_match is None:
            continue

        date_value = datetime.strptime(date_match.group(0), "%B %Y")
        if latest_date is None or date_value > latest_date:
            latest_date = date_value
            latest_href = urljoin(url, href)

    if latest_href is None:
        raise FileNotFoundError("Could not find an Addendum A update on the CMS page.")

    return latest_href


def download_file_from_url_and_extract(url, output_dir=".", expected_extension=".xlsx"):
    latest_href = get_latest_addendum_a_href(url)

    with urllib.request.urlopen(latest_href, timeout=30) as response:
        html = response.read().decode("utf-8")

    zip_match = re.search(r"file=(/files/zip/[^\"']+\.zip)", html, re.IGNORECASE)
    if zip_match is None:
        raise FileNotFoundError("Could not find the Addendum A ZIP file.")

    zip_url = urljoin(url, zip_match.group(1))
    zip_name = os.path.basename(zip_match.group(1))
    zip_path = os.path.join(output_dir, zip_name)
    os.makedirs(output_dir, exist_ok=True)

    urllib.request.urlretrieve(zip_url, zip_path)
    print(f"Downloaded {zip_name} ({os.path.getsize(zip_path)} bytes)")

    selected_file = None
    with ZipFile(zip_path) as zip_file:
        for member in zip_file.namelist():
            extension = os.path.splitext(member)[1].lower()
            if extension != expected_extension.lower():
                continue

            file_name = os.path.basename(member)
            if not file_name:
                continue

            extracted_path = os.path.join(output_dir, file_name)
            output_root = os.path.abspath(output_dir)
            original_path = os.path.abspath(os.path.join(output_dir, member))
            if not original_path.startswith(output_root + os.sep):
                raise ValueError("ZIP contains an unsafe file path.")

            zip_file.extract(member, output_dir)

            if original_path != os.path.abspath(extracted_path):
                os.replace(original_path, extracted_path)

            selected_file = extracted_path
            break

    os.remove(zip_path)

    if selected_file is None:
        raise FileNotFoundError(
            f"No {expected_extension} file was found in the downloaded ZIP."
        )

    print(f"Using extracted file: {selected_file}")
    return selected_file
