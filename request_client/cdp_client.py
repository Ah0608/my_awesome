from playwright.sync_api import sync_playwright

def start_playwright(entry, cdp_url="http://localhost:9222"):
    with sync_playwright() as playwright:
        browser = playwright.chromium.connect_over_cdp(cdp_url)
        entry(browser)
