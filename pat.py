# coding: UTF-8
import requests
import json
import time
from selenium import webdriver
from time import sleep
from tkinter.tix import Form

PAT_LOGIN_URL = 'https://www.ipat.jra.go.jp/pw_020_i.cgi'
IPAT_LOGIN_TAIKEN_URL = 'http://jra.jp/IPAT_TAIKEN/s-pat/pw_080_i.html'

class PatUtil(object):

    def post_login(self):
        values = {
            'uh': '0Lk72o',
            'inetid': 'LN3C08CK',
            'g': '080',
            'u': '6439855492027657',
            'i': '64398554',
            'p': '9202',
            'r': '7657',
            'lf': '1'
        }
        driver = webdriver.PhantomJS()
        driver.get(IPAT_LOGIN_TAIKEN_URL)
        print(driver.current_url)
        i_tag = driver.find_element_by_name('i')
        i_tag.send_keys('12345678')
        p_tag = driver.find_element_by_name('p')
        p_tag.send_keys('2345')
        r_tag = driver.find_element_by_name('r')
        r_tag.send_keys('9876')
#         driver.execute_script('Chk();')
#         button = driver.find_element_by_link_text(' ')
#         button.click()
        form = driver.find_element_by_name('FORM3')
        form.submit()
        sleep(1)
        print(driver.current_url)
        form = driver.find_element_by_name('FORM1')
        form.submit()
        sleep(1)
        print(driver.current_url)
        link = driver.find_element_by_class_name('actionBet')
        link.click()
        sleep(1)

        print(driver.current_url)

if __name__ == '__main__':
    start = time.time()

    pu = PatUtil()
    pu.post_login()
    elapsed_time = time.time() - start
    print('elapsed_time:{0}'.format(elapsed_time) + '[sec]')

