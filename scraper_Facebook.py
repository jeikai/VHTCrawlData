from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
import time

PATH = 'D:/Download/chromedriver-win64/chromedriver-win64/chromedriver.exe'

USERNAME = '0989194097'
PASSWORD = 'phucdepzai123'

service = Service(PATH)
options = webdriver.ChromeOptions()

driver = webdriver.Chrome(service=service, options=options)

login_url = "https://www.facebook.com/login"
driver.get(login_url)

time.sleep(5)

username_field = driver.find_element(By.ID, 'email')
password_field = driver.find_element(By.ID, 'pass')

username_field.send_keys(USERNAME)
password_field.send_keys(PASSWORD)

password_field.send_keys(Keys.RETURN)

time.sleep(5)

target_url = "https://web.facebook.com/langthanghanoiofficial"
driver.get(target_url)

time.sleep(5)

# Scroll down to load more posts
scrolls = 10
for _ in range(scrolls):
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

try:
    xem_them_buttons = driver.find_elements(By.CSS_SELECTOR, 'div.x1i10hfl.xjbqb8w.x1ejq31n.xd10rxx.x1sy0etr.x17r0tee.x972fbf.xcfux6l.x1qhh985.xm0m39n.x9f619.x1ypdohk.xt0psk2.xe8uvvx.xdj266r.x11i5rnm.xat24cr.x1mh8g0r.xexx8yu.x4uap5.x18d9i69.xkhd6sd.x16tdsg8.x1hl2dhg.xggy1nq.x1a2a7pz.x1sur9pj.xkrqix3.xzsf02u.x1s688f[role="button"]')
    for button in xem_them_buttons:
        try:
            button.click()
            time.sleep(1)  
        except:
            continue
except NoSuchElementException:
    pass

resp = driver.page_source
driver.close()

soup = BeautifulSoup(resp, 'html.parser')

def checkExist(modal, element, classname):
    try:
        value = modal.find(element, {'class': classname})
        return value
    except NoSuchElementException:
        return None

posts = soup.find_all('div', {'class': 'x1yztbdb x1n2onr6 xh8yej3 x1ja2u2z'})

post_details = []

for post in posts:
    post_info = {}
    
    content = checkExist(post, 'div', 'xdj266r x11i5rnm xat24cr x1mh8g0r x1vvkbs')
    if content:
        post_info['content'] = content.get_text(strip=True)

    post_details.append(post_info)

for detail in post_details:
    print(detail)
