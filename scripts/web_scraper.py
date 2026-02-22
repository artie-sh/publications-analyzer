import errno
import os
import re
from playwright.sync_api import sync_playwright

DOWNLOAD_DIR = os.path.join(os.path.dirname(__file__), "downloads")
BASE_URL = "https://wsc.nmbe.ch/"
GENUS_URL = "https://wsc.nmbe.ch/genus-catalog/2021/Pardosa"
DOWNLOAD_TIMEOUT = 180_000  # ms
MAX_FILENAME_BYTES = 255
FILENAME_SUFFIX_RESERVE = 10  # room for "_99999.pdf"

RETRY_SUBFOLDERS = {
    "Pardosa_lugubris",
}


def run():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        page.goto(BASE_URL)

        # Wait for login button and click it
        login_link = page.locator("//a[@href='https://wsc.nmbe.ch/user/login']")
        login_link.wait_for(state="visible")
        login_link.click()

        # Fill in credentials and submit
        page.locator("//input[@id='email']").wait_for(state="visible")
        page.locator("//input[@id='email']").fill("artie.sh.87@gmail.com")
        page.locator("//input[@id='password']").fill("K2Dg&8(0g")
        page.locator("//button[contains(text(), 'Login')]").click()

        # Wait for login confirmation
        page.locator("//div[contains(text(), 'Login successful')]").wait_for(state="visible")

        # Navigate to the genus catalog page
        page.goto(GENUS_URL)

        # Collect species titles
        species_items = page.locator("//div[contains(@class, 'species-title')]//em")
        species_items.first.wait_for(state="visible")

        species_count = species_items.count()

        filename_counts = {}
        total_files = 0
        subfolders_processed = 0
        error_subfolders = []

        for i in range(species_count):
            subfolder_name = species_items.nth(i).text_content().replace(" ", "_")
            if subfolder_name not in RETRY_SUBFOLDERS:
                continue
            subfolder_path = os.path.join(DOWNLOAD_DIR, subfolder_name)
            os.makedirs(subfolder_path, exist_ok=True)
            print(f"{i}/{species_count}: {subfolder_name}")

            try:
                # Get reference links for this species (offset by 9)
                ref_links = page.locator(f"(//div[contains(@class, 'tax-ref')])[{i + 10}]//strong/a")
                hrefs = set(ref_links.nth(j).get_attribute("href") for j in range(ref_links.count()))

                for href in hrefs:
                    try:
                        print(f"  {href}")
                        ref_url = BASE_URL.rstrip("/") + "/" + href.lstrip("/")
                        page.goto(ref_url)

                        # Get the author from the reference detail page
                        author_label = page.locator("//tbody//td[contains(text(), 'Author')]")
                        author_label.wait_for(state="visible")
                        author_value = author_label.locator("xpath=following-sibling::td").text_content().strip()

                        year_label = page.locator("//tbody//td[contains(text(), 'Year')]")
                        year_value = year_label.locator("xpath=following-sibling::td").text_content().strip()

                        title_label = page.locator("//tbody//td[contains(text(), 'Title')]")
                        title_value = title_label.locator("xpath=following-sibling::td").text_content().strip()
                        print(f"    Paper: {author_value}, {year_value}, {title_value}")

                        pdf_links = page.locator("//tbody//td[contains(text(), 'PDF')]").locator("xpath=following-sibling::td/a")
                        for k in range(pdf_links.count()):
                            try:
                                pdf_url = pdf_links.nth(k).get_attribute("href")
                                print(f"    PDF: {pdf_url}")

                                base_name = f"{author_value}_{year_value}_{title_value}"
                                base_name = re.sub(r'[^\w\-]', '', base_name.replace(" ", "_"))
                                if base_name in filename_counts:
                                    filename_counts[base_name] += 1
                                    file_name = f"{base_name}_{filename_counts[base_name]}.pdf"
                                else:
                                    filename_counts[base_name] = 0
                                    file_name = f"{base_name}.pdf"
                                file_path = os.path.join(subfolder_path, file_name)
                                if os.path.exists(file_path):
                                    print(f"    Already exists, skipping: {file_name}")
                                else:
                                    download = page.request.get(pdf_url, timeout=DOWNLOAD_TIMEOUT)
                                    try:
                                        with open(file_path, "wb") as f:
                                            f.write(download.body())
                                    except OSError as e:
                                        if e.errno != errno.ENAMETOOLONG:
                                            raise
                                        max_base_bytes = MAX_FILENAME_BYTES - FILENAME_SUFFIX_RESERVE
                                        short_name = base_name.encode('utf-8')[:max_base_bytes].decode('utf-8', errors='ignore')
                                        if filename_counts[base_name] > 0:
                                            file_name = f"{short_name}_{filename_counts[base_name]}.pdf"
                                        else:
                                            file_name = f"{short_name}.pdf"
                                        file_path = os.path.join(subfolder_path, file_name)
                                        with open(file_path, "wb") as f:
                                            f.write(download.body())
                                    print(f"    Downloaded to: {file_name}")
                                total_files += 1
                            except Exception as e:
                                print(f"    ERROR downloading PDF: {e}")
                                if subfolder_name not in error_subfolders:
                                    error_subfolders.append(subfolder_name)
                    except Exception as e:
                        print(f"  ERROR processing ref {href}: {e}")
                        if subfolder_name not in error_subfolders:
                            error_subfolders.append(subfolder_name)

                page.goto(GENUS_URL)
                species_items.first.wait_for(state="visible")
                subfolders_processed += 1
            except Exception as e:
                print(f"  ERROR processing species {subfolder_name}: {e}")
                if subfolder_name not in error_subfolders:
                    error_subfolders.append(subfolder_name)
                try:
                    page.goto(GENUS_URL)
                    species_items.first.wait_for(state="visible")
                except Exception:
                    print("  ERROR: could not navigate back to genus page, stopping")
                    break

        browser.close()

        print("\n=== Summary ===")
        print(f"Species processed: {subfolders_processed}/{species_count}")
        print(f"Total PDF files: {total_files}")
        if error_subfolders:
            print(f"Errors in: {', '.join(error_subfolders)}")
        else:
            print("No errors")


if __name__ == "__main__":
    run()
