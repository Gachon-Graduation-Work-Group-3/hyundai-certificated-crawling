from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import ElementNotInteractableException, NoSuchElementException, ElementClickInterceptedException
import time
import re
import pandas as pd

# ChromeDriver 실행
driver = webdriver.Chrome()
driver.get("https://certified.hyundai.com/p/search/vehicle")

# 페이지 로딩 대기
time.sleep(3)

while True:
    try:
        btn = driver.find_element(By.ID, "btnSeeMore")
        driver.execute_script("arguments[0].scrollIntoView(true);", btn)

        btn.click()
        print("더보기 버튼 클릭 완료")

        time.sleep(0.5)

    except ElementNotInteractableException:
        print("버튼을 클릭할 수 없음! while문 종료.")
        break

    except NoSuchElementException:
        print("[❌] 버튼이 더 이상 존재하지 않음! 중지.")
        break  # 버튼이 사라지면 반복문 종료

    except ElementClickInterceptedException:
        print("[⚠] 버튼 클릭 불가 (다른 요소가 가림). 스크롤 후 재시도.")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)



# JavaScript 이벤트가 포함된 <a> 태그 찾기
elements = driver.find_elements(By.TAG_NAME, "a")

extracted_links = []
# 'href' 속성 확인
for element in elements:
    href = element.get_attribute("href")
    if href and "javascript:common.link.goodsDeatil" in href:
        extracted_id = re.search(r"goodsDeatil\('([^']+)'\)", href).group(1)
        print(f"JavaScript 기반 링크: {extracted_id}")

        extracted_link = f"https://certified.hyundai.com/p/goods/goodsDetail.do?goodsNo={extracted_id}"
        extracted_links.append(extracted_link)

df = pd.DataFrame(extracted_links, columns=["Link"])
df.to_csv("dataset/links.csv", index=False, encoding="utf-8-sig")
print("추출한 링크 csv파일로 저장 완료!")

driver.quit()
