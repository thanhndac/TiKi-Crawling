from __future__ import annotations
import time, json, random
import requests
import pandas as pd
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlsplit, urlunsplit

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# =========================
# Woo headers (according to Data Mapping file)
# =========================
WOO_HEADERS = [
    "ID","Type","SKU","Name","Published","Is featured?","Visibility in catalog",
    "Short description","Description","Date sale price starts","Date sale price ends",
    "Tax status","Tax class","In stock?","Stock","Low stock amount","Backorders allowed?",
    "Sold individually?","Weight(kg)","Length(cm)","Width(cm)","Height(cm)",
    "Allow customer reviews?","Purchase note","Sale price","Regular price","Categories",
    "Tags","Shipping class","Images","Download limit","Download expiry days","Parent",
    "Grouped products","Upsells","Cross-sells","External URL","Button text","Position"
]

# ==== user agents
USER_AGENTS = [
    # Chrome - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.85 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.199 Safari/537.36",
    # Chrome - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.6045.105 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_5_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.88 Safari/537.36",
    # Chrome - Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.5938.92 Safari/537.36",
    # Firefox - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
    # Firefox - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13.6; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12.7; rv:120.0) Gecko/20100101 Firefox/120.0",
    # Firefox - Linux
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:119.0) Gecko/20100101 Firefox/119.0",
    # Edge - Windows
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.6167.85 Safari/537.36 Edg/121.0.2277.83",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.199 Safari/537.36 Edg/120.0.2210.145",
    # Safari - macOS
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 12_7_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    # Brave
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Brave Chrome/119.0.6045.160 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_3_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.97 Safari/537.36 Brave/1.58.127",
    # Opera
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.6099.199 Safari/537.36 OPR/96.0.4693.80"
]

# =========================
# Product Class
# =========================
class Product:
    def __init__(self, **kwargs):
        for h in WOO_HEADERS:
            setattr(self, h, "")
        for k, v in kwargs.items():
            setattr(self, k, v)

    def to_dict(self) -> dict:
        return {h: getattr(self, h, "") for h in WOO_HEADERS}

# =========================
# Requests Session + retry
# =========================
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update({
        "User-Agent": random.choice(USER_AGENTS),   # pick ONE UA
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8",
        "Referer": "https://tiki.vn/",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })
    try:
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter
        retry = Retry(total=3, backoff_factor=0.5,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=["GET"])
        adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
    except Exception:
        pass
    return s

SESSION = build_session()

def get_page(url: str, timeout: int = 10) -> str:
    r = SESSION.get(url, timeout=timeout)
    r.raise_for_status()
    return r.text

# =========================
# JSON-LD helpers
# =========================
def parse_ldjson_all(soup: BeautifulSoup) -> list[dict]:
    objs = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = json.loads(raw.strip())
        if isinstance(data, dict) and isinstance(data.get("@graph"), list):
            objs.extend(data["@graph"])
        elif isinstance(data, list):
            objs.extend(data)
        else:
            objs.append(data)
    return objs

def pick_product_obj(ldjson_objs: list[dict]) -> dict | None:
    for o in ldjson_objs:
        t = o.get("@type")
        if (isinstance(t, str) and t.lower() == "product") or (isinstance(t, list) and "Product" in t):
            return o
    return None

def availability_to_bool(avail_url: str | None) -> bool | None:
    if not avail_url:
        return None
    return avail_url.split("/")[-1].lower() == "instock"

# =========================
# Build Product from URL
# =========================
def build_product_from_url(url: str) -> Product:
    u = urlsplit(url)
    clean = urlunsplit((u.scheme, u.netloc, u.path, "", ""))

    html = get_page(clean)
    soup = BeautifulSoup(html, "html.parser")

    meta_product = soup.find("meta", attrs={"name": "product"})
    pid_meta = (meta_product.get("content") or "").strip() if meta_product else ""

    title_tag = soup.find("title")
    page_title = title_tag.get_text(strip=True) if title_tag else ""

    meta_desc = soup.find("meta", attrs={"name": "description"})
    page_desc = (meta_desc.get("content") or "").strip() if meta_desc else ""

    ld = parse_ldjson_all(soup)
    prod = pick_product_obj(ld) or {}

    name = prod.get("name") or page_title
    sku  = prod.get("sku") or ""
    desc = (prod.get("description") or page_desc).strip() if (prod.get("description") or page_desc) else ""

    image_obj = prod.get("image")
    if isinstance(image_obj, dict):
        image_url = image_obj.get("url") or ""
    elif isinstance(image_obj, str):
        image_url = image_obj
    else:
        image_url = ""

    offers = prod.get("offers", {}) if isinstance(prod.get("offers"), dict) else {}
    sale_price = offers.get("price")
    price_valid_until = offers.get("priceValidUntil", "")
    avail = offers.get("availability", "")
    in_stock = availability_to_bool(avail)

    reg_price = ""
    price_spec = offers.get("priceSpecification", {}) if isinstance(offers.get("priceSpecification"), dict) else {}
    if price_spec.get("priceType", "").endswith("StrikethroughPrice"):
        reg_price = str(price_spec.get("price") or "")

    product = Product(
        ID=pid_meta,
        Type="simple",
        SKU=sku,
        Name=name,
        Published="1",
        **{
            "Is featured?": "0",
            "Visibility in catalog": "visible",
            "Short description": "",
            "Description": desc,
            "Date sale price starts": "",
            "Date sale price ends": price_valid_until,
            "Tax status": "taxable",
            "Tax class": "",
            "In stock?": "1" if in_stock else ("0" if in_stock is not None else ""),
            "Stock": "",
            "Low stock amount": "",
            "Backorders allowed?": "no",
            "Sold individually?": "no",
            "Weight(kg)": "",
            "Length(cm)": "",
            "Width(cm)": "",
            "Height(cm)": "",
            "Allow customer reviews?": "1",
            "Purchase note": "",
            "Sale price": str(sale_price) if sale_price is not None else "",
            "Regular price": reg_price,
            "Categories": "",
            "Tags": "",
            "Shipping class": "",
            "Images": image_url,
            "Download limit": "",
            "Download expiry days": "",
            "Parent": "",
            "Grouped products": "",
            "Upsells": "",
            "Cross-sells": "",
            "External URL": "",
            "Button text": "",
            "Position": "0",
        }
    )
    return product

# =========================
# Selenium: collect URLs
# =========================
def scrape_category_urls(category_url: str, times_click_more: int = 20) -> list[str]:
    opts = Options()
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=opts)
    driver.get(category_url)
    time.sleep(3)

    for i in range(times_click_more):
        try:
            btn = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//div[@data-view-id="category_infinity_view.more"]'))
            )
            btn.click()
            print(f"Clicked 'Xem thêm' ({i+1}/{times_click_more})")
            time.sleep(1.5)
        except Exception:
            print("No more 'Xem thêm' or timeout; stop clicking.")
            break

    anchors = driver.find_elements(By.XPATH, '//a[contains(@class, "product-item")]')
    urls = []
    for a in anchors:
        try:
            href = a.get_attribute("href")
            if href and href.startswith("https"):
                urls.append(href)
        except Exception:
            pass

    driver.quit()

    cleaned, seen = [], set()
    for u in urls:
        s = urlsplit(u)
        no_q = urlunsplit((s.scheme, s.netloc, s.path, "", ""))
        if no_q not in seen:
            seen.add(no_q)
            cleaned.append(no_q)

    print(f"Collected {len(cleaned)} unique product URLs.")
    return cleaned

# =========================
# Concurrency helpers
# =========================
def fetch_all_products(urls: list[str], max_workers: int = 8, jitter=(0.05, 0.2)) -> list[dict]:
    """Fetch a batch of product URLs concurrently (8 threads)."""
    rows = []

    def _task(u: str):
        time.sleep(random.uniform(*jitter))  # small polite jitter
        p = build_product_from_url(u)
        return p.to_dict()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(_task, u) for u in urls]
        for f in as_completed(futures):
            try:
                rows.append(f.result())
            except Exception as e:
                print("[ERR]", e)
    return rows

def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

def process_in_batches(urls: list[str], per_batch_limit: int = 240, cooldown_sec: int = 31,
                       max_workers: int = 8) -> list[dict]:
    """
    Respect Tiki rate limit: max 240 requests then cooldown 31 seconds.
    Process in batches with 8 threads to stay safe.
    """
    all_rows = []
    batches = list(chunked(urls, per_batch_limit))
    for idx, batch in enumerate(batches, start=1):
        print(f"\n== Batch {idx}/{len(batches)}: {len(batch)} URLs ==")
        rows = fetch_all_products(batch, max_workers=max_workers)
        all_rows.extend(rows)
        if idx < len(batches):
            print(f"Hit {len(batch)} requests. Cooling down {cooldown_sec}s to respect Tiki limit...")
            time.sleep(cooldown_sec)
    return all_rows

# =========================
# Main
# =========================
def main():
    category_url = "https://tiki.vn/nha-cua-doi-song/c1883"

    urls = scrape_category_urls(category_url, times_click_more=20)

    product_rows = process_in_batches(
        urls=urls,
        per_batch_limit=240,   # Tiki limit
        cooldown_sec=31,
        max_workers=8          # <-- 8 threads
    )

    df = pd.DataFrame(product_rows, columns=WOO_HEADERS)
    out = "tiki_product_crawling.csv"
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nSaved {len(df)} products to {out}")

if __name__ == "__main__":
    main()
