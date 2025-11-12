from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import TimeoutException
import time
import json
from datetime import datetime
import os
from selenium.webdriver.chrome.service import Service


RECYCLER_SELECTOR = ("#app > div > div.container.jb-container.container-sidebar--rwd.main.pt-1.pt-md-5"
                     " > div > div.col.main > div.job > div.vue-recycle-scroller.ready.page-mode.direction-vertical.recycle-scroller")
ITEM_WRAPPER_SEL = RECYCLER_SELECTOR + " > div.vue-recycle-scroller__item-wrapper"
CARD_CHILD_SEL = ITEM_WRAPPER_SEL + " > div"

MAX_SCROLLS = 60
SCROLL_PAUSE = 0.35
SNAPSHOT_DIR = "snapshots"


def _save_snapshot(self, name_prefix="snapshot"):
    os.makedirs(SNAPSHOT_DIR, exist_ok=True)
    ts = int(time.time())
    html_path = os.path.join(SNAPSHOT_DIR, f"{name_prefix}_{ts}.html")
    png_path = os.path.join(SNAPSHOT_DIR, f"{name_prefix}_{ts}.png")
    try:
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(self.driver.page_source[:1000000])
    except Exception as e:
        print("Failed to save HTML snapshot:", e)
    try:
        self.driver.save_screenshot(png_path)
    except Exception as e:
        print("Failed to save screenshot:", e)
    print(f"Saved snapshot: {html_path}, screenshot: {png_path}")


def collect_vrt_cards(self, max_scrolls=MAX_SCROLLS, scroll_pause=SCROLL_PAUSE):
    try:
        recycler = self.wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, RECYCLER_SELECTOR)))
    except TimeoutException:
        print("Recycler element not found with CSS selector; saving snapshot for inspection.")
        _save_snapshot(self, "no_recycler")
        return []

    try:
        wrapper = recycler.find_element(By.CSS_SELECTOR, "div.vue-recycle-scroller__item-wrapper")
    except Exception:
        try:
            wrapper = self.driver.find_element(By.CSS_SELECTOR, ITEM_WRAPPER_SEL)
        except Exception:
            print("Item wrapper not found inside recycler.")
            _save_snapshot(self, "no_wrapper")
            return []
    
    seen = set()
    cards=[]
    
    def harvest():
        elems = wrapper.find_elements(By.CSS_SELECTOR,  CARD_CHILD_SEL)
        if not elems:
            elems = self.driver.find_elements(By.CSS_SELECTOR, CARD_CHILD_SEL)
        new_found = 0
        for el in elems:
            fid = (
                el.get_attribute("data-key")
                or el.get_attribute("data-v-job-id")
                or el.get_attribute("data-job-id")
                or el.get_attribute("id")
            )
            if not fid:
                try:
                    outer = el.get_attribute("outerHTML") or ""
                    fid = outer[:250]
                except Exception:
                    fid = f"pyid:{id(el)}"
            if fid not in seen:
                seen.add(fid)
                cards.append(el)
                new_found += 1
        return new_found
    
    harvest()
    
    scrolls_done = 0
    for _ in range(max_scrolls):
        try:
            self.driver.execute_script(
                "const sc = arguments[0]; sc.scrollTop = sc.scrollTop + Math.max(sc.clientHeight, 600);",
                recycler,
            )
        except Exception:
            try:
                self.driver.execute_script(
                    "const sc = arguments[0]; sc.scrollTop = sc.scrollTop + Math.max(sc.clientHeight, 600);",
                    wrapper,
                )
            except Exception:
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")

        time.sleep(scroll_pause)
        new = harvest()
        scrolls_done += 1

        if new == 0:
            time.sleep(0.25)
            new = harvest()
            if new == 0:
                break

    try:
        self.driver.execute_script("arguments[0].scrollTop = 0;", recycler)
    except Exception:
        try:
            self.driver.execute_script("arguments[0].scrollTop = 0;", wrapper)
        except Exception:
            pass

    print(f"collect_virtualized_cards: collected {len(cards)} cards after {scrolls_done} scrolls")
    return cards
        

class Job104Scraper:
    def __init__(self, headless=True) -> None:
        self.base_url = "https://www.104.com.tw/jobs/search/"
        self.setup_driver(headless)
        
    def setup_driver(self, headless):
        remote = os.getenv("CHROME_REMOTE_URL")
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        if os.path.exists('/usr/bin/google-chrome'):
            chrome_options.binary_location = '/usr/bin/google-chrome'
        
        try:
            service = Service('/usr/local/bin/chromedriver')
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, 15)
            
            print("Successfully started Chrome in GitHub Actions")
            
        except Exception as e:
            print(f"Chrome driver start failed: {e}")
            try:
                self.driver = webdriver.Chrome(options=chrome_options)
                self.wait = WebDriverWait(self.driver, 15)
                print("Successfully started Chrome with fallback method")
            except Exception as e2:
                print(f"Fallback also failed: {e2}")
                raise
       
        
    def build_search_url(self, keywords, location="台灣", job_type="實習"):
        keyword_encoded = keywords.replace(" ", "+")
        url  =f"{self.base_url}?ro=0&keyword={keyword_encoded}&jobsource=intern&order=1"
        return url
    
    def scrape_jobs(self, keywords_list, max_pages=4):
        all_jobs = []
        for keyword in keywords_list:
            print(f"\n Searching for: {keyword}")
            search_url = self.build_search_url(keyword)
            try:
                self.driver.get(search_url)
            except Exception as e:
                print("Driver.get failed:", e)
                continue

            time.sleep(1.0) 

            try:
                for sel in ["button#onetrust-accept-btn-handler", "button.cookie-accept", "button[aria-label*='close']"]:
                    try:
                        b = self.driver.find_element(By.CSS_SELECTOR, sel)
                        if b and b.is_displayed():
                            b.click()
                            time.sleep(0.4)
                    except Exception:
                        continue
            except Exception:
                pass

            cards = collect_vrt_cards(self)
            if not cards:
                print("No cards collected; snapshot saved for inspection.")
                _save_snapshot(self, f"no_cards_{keyword.replace(' ', '_')}")
                continue

            for card in cards:
                try:
                    job = self.extract_job_data(card, keyword)
                    if job:
                        all_jobs.append(job)
                except Exception as e:
                    print("extract error:", e)
                    continue

        print(f"\n Total jobs scraped: {len(all_jobs)}")
        return all_jobs
        

    def extract_job_data(self,card, search_keyword):
        def try_select_text(sel):
            try:
                el = card.find_element(By.CSS_SELECTOR, sel)
                txt = el.text.strip()
                return txt if txt else None
            except Exception:
                return None
        
        title_selectors = [
            "div.info > div > div.info-job.text-break.mb-2",   
            ".info-job.text-break.mb-2",
            "div.info-job",
            "h2, .job-title, .info > h2"
        ]
            
        title = None
        for s in title_selectors:
            title = try_select_text(s)
            if title:
                break

        
        job_url = ""
        try:
            title_anchor = card.find_element(By.CSS_SELECTOR, "div.info > div > div.info-job.text-break.mb-2 a")
            job_url = title_anchor.get_attribute("href") or ""
        except Exception:
            try:
                a = card.find_element(By.TAG_NAME, "a")
                job_url = a.get_attribute("href") or ""
            except Exception:
                job_url = ""
        
        company_selectors = [
        "div.info > div > div.info-company.mb-1",
        ".info-company.mb-1",
        ".info-company",
        ".company, .job-company"
        ]
        
        company = None
        for s in company_selectors:
            company = try_select_text(s)
            if company:
                break
        if not company:
            company = "Unknown"
            
        location = try_select_text("div.info > div > div.info-tags.gray-deep-dark > span:nth-child(1)")
        if not location:
            try:
                spans = card.find_elements(By.CSS_SELECTOR, "div.info .info-tags.gray-deep-dark > span")
                for sp in spans:
                    t = sp.text.strip()
                    if t:
                        location = t
                        break
            except Exception:
                location = None
        if not location:
            location = "台灣"
            
        salary = None
        try:
            salary = try_select_text("div.info > div > div.info-tags.gray-deep-dark > span:nth-child(4) > a")
        except Exception:
            salary = None

        if not salary:
            try:
                spans = card.find_elements(By.CSS_SELECTOR, "div.info .info-tags.gray-deep-dark > span")
                if len(spans) >= 4:
                    salary = spans[3].text.strip()
            except Exception:
                salary = None

        if not salary:
            try:
                all_text = card.text
                if "面議" in all_text:
                    salary = "面議"
                else:
                    for marker in ["$", "NT", "月薪", "年薪", "TWD", "面議"]:
                        if marker in all_text:
                            idx = all_text.find(marker)
                            snippet = all_text[max(0, idx-20): idx+40]
                            salary = snippet.strip().split("\n")[0]
                            break
            except Exception:
                salary = None

        if not salary:
            salary = "面議"
        
        description_selectors = [
        "div.info > div > div.info-description.text-gray-darker.t4.text-break.mt-2.position-relative.info-description__line2",
        ".info-description",
        ".info-description.text-gray-darker",
        ".job-snippet, .description"
        ]
        
        description = None
        for s in description_selectors:
            description = try_select_text(s)
            if description:
                break
        if not description:
            description = ""
            
        date_posted = try_select_text("div.col-auto.date > div") or try_select_text(".col-auto.date > div") or try_select_text(".date")
        if not date_posted:
            date_posted = "Unknown"
            
        return {
            "title": title,
            "company": company,
            "location": location or "台灣",
            "url": job_url,
            "salary": salary,
            "description": description,
            "date_posted": date_posted,
            "search_keyword": search_keyword,
            "source": "104.com.tw",
            "scraped_at": datetime.now().isoformat()
        }
        
    def save_to_json(self, jobs, filename='104_jobs.json'):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        print(f"Saved {len(jobs)} jobs to {filename}")

    def close(self):
        if hasattr(self, 'driver'):
            self.driver.quit()
        
if __name__ == "__main__":
    scraper = Job104Scraper(headless=False)
    
    try:
        keywords =[
            "AI工程師 實習",
            "前端工程師 實習", 
            "後端工程師 實習",
            "機器學習 實習"
        ]
        
        jobs = scraper.scrape_jobs(keywords, max_pages=4)
        
        print("\n" + "="*50)
        for i, job in enumerate(jobs[:5], 1):
            print(f"{i}. {job['title']}")
            print(f"Company: {job['company']}")
            print(f"Location: {job['location']}")
            print(f"Salary: {job['salary']}")
            print(f"URL: {job['url']}\n")
        scraper.save_to_json(jobs)
        
        print(f"Scraping completed! Found {len(jobs)} internship positions")
    
    except Exception as e:
        print(f"error during scraping {e}")

    finally:
        scraper.close()
        print("Scraper closed")
