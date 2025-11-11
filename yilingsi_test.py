from selenium import webdriver
from selenium.webdriver.chrome.options import Options

options = Options()
options.binary_location = r"C:\Program Files\Google\Chrome\Application\chrome.exe"

driver = webdriver.Chrome(options=options) 
driver.get("https://www.104.com.tw")
print("✅ ChromeDriver works!")
driver.quit()