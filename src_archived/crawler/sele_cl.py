import time
import platform
import os

from dateutil import tz
from selenium import webdriver
from selenium.webdriver import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.proxy import Proxy, ProxyType

import datetime

from core.app_base import AppBase
from libs.storage_utils import get_mongo_cli

local_tz = tz.tzlocal()

os_system = platform.system()

if os_system == 'Linux':
    chrome_drive_exec = "chromedriver_linux"
elif os_system == 'Windows':
    chrome_drive_exec = "chromedriver.exe"
else:
    chrome_drive_exec = "chromedriver_mac"

KEY_CONTROL = {
    "Darwin": Keys.COMMAND,
}.get(os_system, Keys.CONTROL)

PROXY = "torproxy:8118"


class SeleCl(AppBase):

    def __init__(self, config):
        super(SeleCl, self).__init__(config)
        mongo_conf = self.get_param_config(['db', 'mongo'])
        self.postgres_conf = self.get_param_config(['db', 'postgres'])
        self.chrome_driver_path = self.get_param_config(['chrome_driver_path'])
        self.driver_headless = self.get_param_config(['driver_headless'])
        self.enable_proxy = self.get_param_config(['enable_proxy'])
        self.mongo_cli = get_mongo_cli(mongo_conf)
        # self.from_date = self.get_process_info(['from_date'])
        # self.to_date = self.get_process_info(['to_date'])

    def create_chrome_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--incognito")
        chrome_options.add_argument("--window-size=1920x1080")
        if self.driver_headless:
            chrome_options.add_argument("--headless")
        capabilities = webdriver.DesiredCapabilities.CHROME
        if self.enable_proxy:
            prox = Proxy()
            prox.proxy_type = ProxyType.MANUAL
            prox.http_proxy = PROXY
            prox.ssl_proxy = PROXY
            prox.add_to_capabilities(capabilities)
            # chrome_options.add_argument('--proxy-server=%s' % PROXY)

        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        executable_path = os.path.join(self.chrome_driver_path, chrome_drive_exec)
        driver = webdriver.Chrome(options=chrome_options,
                                  executable_path=executable_path,
                                  desired_capabilities=capabilities)

        return driver

    @staticmethod
    def check_and_get_by_xpath(driver,  xpath):
        try:
            web_element = driver.find_element_by_xpath(xpath)
            # webdriver.ActionChains(driver).move_to_element(web_element).perform()
        except NoSuchElementException:
            return None
        return web_element.text

    def crawler(self, driver):
        driver.get("https://www.careerlink.vn/")
        main_window = driver.current_window_handle
        driver.get("https://www.careerlink.vn/nguoi-tim-viec/login")
        driver.find_element_by_xpath(".//input[(contains(@type, 'email'))]").send_keys("hunglmuit@gmail.com")
        driver.find_element_by_xpath(".//input[(contains(@type, 'password'))]").send_keys("yhVqPf#Jp3eQ4vE")
        driver.find_element_by_xpath(".//button[(contains(@type, 'submit'))]").click()

        for i in range(2, 30):
            url = "https://www.careerlink.vn/en/job/list?page={}".format(i)
            print(url)
            driver.get(url)
            time.sleep(5)

            # list_item = []

            # Store the ID of the original window
            page_window = driver.current_window_handle

            list_job = driver.find_elements(By.XPATH, "//ul/li[(contains(@class, 'job-item'))]")

            # captcha_element = driver.find_elements(By.XPATH, "//div[(contains(@class, 'recaptcha-checkbox-borderAnimation'))]")
            # ActionChains(driver).click(captcha_element).perform()

            print(type(list_job))
            time.sleep(2)
            for web_element_job in list_job:
                webdriver.ActionChains(driver).move_to_element(web_element_job).perform()
                # todo: parse -> item
                meta = web_element_job.get_attribute('class')
                is_tlp_job = 'tlp-job' in meta
                is_hot_job = 'hot-job' in meta

                job_element = web_element_job.find_element_by_xpath(".//a[(contains(@class, 'job-link'))]")
                is_red_job = 'red' in job_element.find_element_by_xpath("./h5").get_attribute('class')
                name = job_element.get_attribute('title')
                url = web_element_job.find_element_by_class_name("job-link").get_attribute('href')
                _id = url.split("/")[-1]

                company_element = web_element_job.find_element_by_xpath(".//a[(contains(@class, 'job-company'))]")
                company_name = company_element.get_attribute('title')
                # todo
                company_url = web_element_job.find_element_by_class_name("job-link").get_attribute('href')
                company_id = company_url.split("/")[-1]

                areas = [i.text for i in
                         web_element_job.find_elements_by_xpath(".//div[(contains(@class, 'job-location'))]/div")]
                salary = [i.text for i in
                          web_element_job.find_elements_by_xpath(".//span[(contains(@class, 'job-salary'))]")][
                    -1].strip()
                position = web_element_job.find_element_by_xpath(
                    ".//a[(contains(@class, 'job-position'))]").text.strip()
                job_updated_at = int(web_element_job.find_element_by_xpath(
                    ".//span[(contains(@class, 'job-update-time'))]/span").get_attribute('data-datetime'))
                job_updated_at = datetime.datetime.fromtimestamp(job_updated_at, local_tz)

                item = {
                    "id": _id,
                    "name": name,
                    "url": url,
                    "company_id": company_id,
                    "company_name": company_name,
                    "company_url": company_url,
                    "areas": areas,
                    "salary": salary,
                    "position": position,
                    "is_tlp_job": is_tlp_job,
                    "is_hot_job": is_hot_job,
                    "is_red_job": is_red_job,
                    "job_updated_date": job_updated_at.replace(tzinfo=local_tz),
                    'sourcetype': 'cl',
                    'objectID': "cl_" + _id
                }

                # open new tab with link: url
                # web_element_job.send_keys(Keys.CONTROL,Keys.ENTER)
                ActionChains(driver) \
                    .key_down(KEY_CONTROL) \
                    .click(web_element_job) \
                    .key_up(KEY_CONTROL) \
                    .perform()

                time.sleep(2)
                # Wait for the new window or tab
                # wait.until(EC.number_of_windows_to_be(2))

                # switch to new tab.
                for window_handle in driver.window_handles:
                    if window_handle != main_window and window_handle != page_window:
                        driver.switch_to.window(window_handle)
                        break
                time.sleep(5)
                # job_window = driver.current_window_handle

                try:
                    if driver.find_element_by_xpath("//div[@class='form-group recapcha-container']"):
                        # todo resolve captcha
                        print("got captcha")
                        print("pending 360s")
                        time.sleep(360)
                        box_click_element = driver.find_element_by_xpath(".//div[(contains(@class, 'recapcha-container'))]")
                        action = ActionChains(driver)
                        action.move_to_element_with_offset(box_click_element, 40, 40)
                        action.click()
                        action.perform()
                        print("Click captcha")
                        time.sleep(10)
                except:
                    print("don't got captcha")

                updated_at = datetime.datetime.now().replace(tzinfo=local_tz)
                item['updated_at'] = updated_at
                job_description = "\n".join([i.text for i in driver.find_elements_by_xpath(
                    "//div[(@class='raw-content') and (@itemprop='description')]/p")])
                job_requirement = "\n".join([i.text for i in driver.find_elements_by_xpath(
                    "//div[(@class='raw-content') and (@itemprop='skills')]/p")])
                k = list(map(lambda x: x.replace(' ', '_').lower(), [i.text for i in driver.find_elements_by_xpath(
                    "//div[(contains(@class, 'job-summary-item'))]/div[@class='my-0 summary-label']")][:-1]))
                v = list(filter(
                    lambda x: len(x) > 3,
                    map(lambda x: x.strip(),
                        [i.text for i in driver.find_elements_by_xpath(
                            "//div[(contains(@class, 'job-summary-item'))]/div[@class='font-weight-bolder']")]
                        )
                ))
                job_detail = dict(zip(k, v))
                job_fields = [i.text for i in driver.find_elements_by_xpath(
                    "//div[(contains(@class, 'job-summary-item'))]/div[@class='font-weight-bolder']/a/span")]
                job_detail['job_fields'] = list(map(lambda x: x.strip(), job_fields))
                job_detail['job_description'] = job_description
                job_detail['job_requirement'] = job_requirement

                contact_name = self.check_and_get_by_xpath(driver, "//span[(contains(@class, 'person-name'))]")
                posted_at = self.check_and_get_by_xpath(driver, "//span[@itemprop='datePosted']")
                expired_at = self.check_and_get_by_xpath(driver, "//span[@itemprop='validThrough']")
                company_size = self.check_and_get_by_xpath(driver, "//span[@itemprop='numberOfEmployees']")
                company_profile = "\n".join([i.text for i in driver.find_elements_by_xpath(
                    "//div[(contains(@class, 'company-profile'))]/p/strong")])

                job_street_address = self.check_and_get_by_xpath(driver, "//span[@itemprop='streetAddress']")
                job_district = self.check_and_get_by_xpath(driver, "//span[@itemprop='addressLocality']")
                job_province = self.check_and_get_by_xpath(driver, "//span[@itemprop='addressRegion']")
                job_country = self.check_and_get_by_xpath(driver, "//span[@itemprop='addressCountry']")

                job_tags = [i.text for i in driver.find_elements_by_xpath(
                    "//div[@class='tags-container']/a")]
                job_detail['contact_name'] = contact_name
                job_detail['posted_at'] = posted_at
                job_detail['expired_at'] = expired_at
                job_detail['company_size'] = company_size
                job_detail['company_profile'] = company_profile

                job_detail['job_street_address'] = job_street_address
                job_detail['job_district'] = job_district
                job_detail['job_province'] = job_province
                job_detail['job_country'] = job_country

                job_detail['job_tags'] = job_tags
                item.update(job_detail)
                print(item)
                self.mongo_cli['crawlab']['sele_cl'].find_one_and_update(
                    {"objectID": item['objectID']}, {"$set": item}, upsert=True
                )
                # list_item.append(item)
                # company_address = driver.find_element_by_class_name("text-dark-gray").text
                # print(company_address)
                driver.close()
                # close tab => getback
                driver.switch_to.window(page_window)
                # driver.refresh()
                time.sleep(3)

            # todo process after read one page
            # print(list_item)

        driver.close()

    def execute(self):
        self.log.info("Start Execute")
        driver = self.create_chrome_driver()
        print("Start")
        self.crawler(driver=driver)
        print("Finish")
        self.mongo_cli.close()
