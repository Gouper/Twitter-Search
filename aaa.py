from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
import time

browser = webdriver.Chrome() #加载Chrome浏览器
# browser.maximize_window() #最大化
browser.get('https://twitter.com/realDonaldTrump/status/682805320217980929') #加载该页面
for i in range(10):
    # setscroll = "document.documentElement.scrollTop=" + "500"
    # browser.execute_script(setscroll)
    #
    # Drag = browser.find_element_by_class_name("jspDrag") #找到滚动条
    # #控制滚动条的行为，每次向y轴(及向下)移动10个单位
    # ActionChains(browser).drag_and_drop_by_offset(Drag, 0, 10).perform()
    ActionChains(browser).key_down(Keys.PAGE_DOWN).perform()
    time.sleep(2) #休眠2秒
browser.close()


# driver.execute_script('window.scrollTo(0, document.body.scrollHeight);')