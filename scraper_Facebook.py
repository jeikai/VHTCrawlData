from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import time
from urllib.parse import urljoin

class FacebookScraper:
    def __init__(self, username, password, driver_path, scrolls=10):
        self.username = username
        self.password = password
        self.driver_path = driver_path
        self.page_count = 0
        self.service = Service(driver_path)
        self.options = webdriver.ChromeOptions()
        self.driver = webdriver.Chrome(service=self.service, options=self.options)
        
    def login(self, target_url):
        login_url = "https://www.facebook.com/login"
        self.driver.get(login_url)

        username_field = self.driver.find_element(By.ID, 'email')
        password_field = self.driver.find_element(By.ID, 'pass')

        username_field.send_keys(self.username)
        password_field.send_keys(self.password)

        password_field.send_keys(Keys.RETURN)
        time.sleep(3)
        
        self.driver.get(target_url)

        try:
            timeline_link = self.driver.find_element(By.XPATH, '//a[contains(text(),"Dòng thời gian")]')
            timeline_link.click()
            time.sleep(2) 
            current_url = self.driver.current_url
            self.scrape_page(current_url)
        except NoSuchElementException:
            print("The 'Dòng thời gian' link was not found.")
            self.close()
        
    def scrape_page(self, url):
        if self.page_count >= 5:
            return

        self.driver.get(url)
        time.sleep(2)
        resp = self.driver.page_source
        soup = BeautifulSoup(resp, 'html.parser')

        post_links = soup.select('a:contains("Toàn bộ tin")')
        
        for link in post_links:
            post_url = link['href']
            post_soup = self.get_soup_from_url(post_url)
            self.extract_posts(post_soup)

        next_page_link = soup.select_one('a:contains("Xem tin khác")')
        if next_page_link:
            self.page_count += 1
            next_page_url = urljoin('https://mbasic.facebook.com', next_page_link['href'])
            self.scrape_page(next_page_url)
        
    def get_soup_from_url(self, url):
        full_url = f"https://mbasic.facebook.com{url}"
        self.driver.get(full_url)
        time.sleep(2)
        page_source = self.driver.page_source
        soup = BeautifulSoup(page_source, 'html.parser')
        return soup

    def extract_posts(self, soup):
        print(soup)

    def close(self):
        self.driver.close()

# Usage
if __name__ == "__main__":
    USERNAME = '0989194097'
    PASSWORD = 'phucdepzai123'
    PATH = 'D:/Download/chromedriver-win64/chromedriver-win64/chromedriver.exe'
    
    scraper = FacebookScraper(USERNAME, PASSWORD, PATH)
    scraper.login("https://mbasic.facebook.com/VTVcab.Tintuc")
    scraper.close()
