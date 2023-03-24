from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options


class FundTransferAutomation:
    def __init__(self, coin="ADA", amount="100"):
        self.coin = coin
        self.amount = amount
        self.driver = None

    def setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument('--disable-gpu')
        self.driver = webdriver.Chrome(executable_path='path/to/chromedriver', options=chrome_options)

    def navigate_to_website(self):
        self.driver.get('https://shendeng.bitapple.com/#/fund-transfer/allo-transfer-form')

    def wait_and_locate_element(self, by, value, action=None, send_keys=None):
        element = WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((by, value))
        )
        if action:
            action(element)
        if send_keys:
            element.send_keys(send_keys)
        return element

    def create_new_transfer(self):
        self.wait_and_locate_element(By.XPATH, '//button[contains(text(), "创建新的划转单")]', action=lambda e: e.click())

    def select_transfer_path_and_reason(self):
        self.wait_and_locate_element(By.XPATH, '//select[@id="transferPath"]', send_keys="bitrue-1638094=>binance-orange")
        self.wait_and_locate_element(By.XPATH, '//select[@id="transferReason"]', send_keys="对冲配资")

    def select_currency(self):
        self.wait_and_locate_element(By.XPATH, '//button[contains(text(), "选择币种")]', action=lambda e: e.click())

    def select_specific_currency(self):
        self.wait_and_locate_element(By.XPATH, f'//button[contains(text(), "{self.coin}")]', action=lambda e: e.click())

    def select_chain_and_fill_transfer_amount(self):
        self.wait_and_locate_element(By.XPATH, '//select[@id="coinChain"]', send_keys=self.coin)
        self.wait_and_locate_element(By.XPATH, '//input[@id="transferAmount"]', send_keys=self.amount)

    def confirm_transfer(self):
        self.wait_and_locate_element(By.XPATH, '//button[contains(text(), "确认")]', action=lambda e: e.click())

    def back_and_confirm(self):
        self.wait_and_locate_element(By.XPATH, '//button[contains(text(), "返回")]', action=lambda e: e.click())
        self.wait_and_locate_element(By.XPATH, '//button[contains(text(), "确认")]', action=lambda e: e.click())

    def submit_transfer(self):
        self.wait_and_locate_element(By.XPATH, '//button[contains(text(), "提交")]', action=lambda e: e.click())

    def quit_driver(self):
        self.driver.quit()

    def run(self):
        self.setup_driver()
        self.navigate_to_website()
        self.create_new_transfer()
        self.select_transfer_path_and_reason()
        self.select_currency()
        self.select_specific_currency()
        self.select_chain_and_fill_transfer_amount()
        self.confirm_transfer()
        self.back_and_confirm()
        self.submit_transfer()
        self.quit_driver()


if __name__ == "__main__":
    x = FundTransferAutomation()
    x.run()
