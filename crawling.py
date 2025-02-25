import asyncio
import aiohttp
import pandas as pd
from bs4 import BeautifulSoup
from lxml import etree

# 🚗 CSV 파일 불러오기
df = pd.read_csv("dataset/links.csv")

# 링크 컬럼이 "link"라는 가정
urls = df["Link"].tolist()

MAX_CONCURRENT_REQUESTS = 50
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)


"""URL에서 HTML 가져오기"""
# 🚀 비동기 요청을 수행할 차량 정보 크롤러
async def fetch(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                return await response.text()
            else:
                print(f"{url} 응답 오류: {response.status}")
                return None
            
    except Exception as e:
        print(f"{url} 요청 실패: {e}")
    
    await asyncio.sleep(2)

    return None
    

"""HTML에서 피쳐들 파싱"""
async def parse_car_info(html, url):
    if not html:
        return None
    
    print(url)
    
    soup = BeautifulSoup(html, "lxml")
    tree = etree.HTML(str(soup))


    # 차량 정보 추출
    try:
        # 링크
        link = url
        # 이름
        name = tree.xpath("//*[@id='CPOcontents']/div[1]/div[2]/div[1]/div[1]/div[2]/text()")
        name = name[0].strip() if name else None
        
        # 가격
        price = tree.xpath('//*[@id="payArea1"]/em/text()')
        price = price[0].strip() if price else None

        # 신차 가격
        new_car_price = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[3]/div/div[2]/div/span[2]/span/text()')
        new_car_price = new_car_price[0].strip() if new_car_price else None

        # # 신차 대비 가격 -> 전처리할 때 계산해야 함.
        # if new_car_price is not None and price is not None and price != 0:
        #     # price = int(price)
        #     # new_car_price = int(new_car_price)
        #     # new_car_percent = round(new_car_price / price * 100, 0)  # 백분율로 변환 후 소수점 0자리 반올림
        # else:
        #     new_car_percent = None
        new_car_percent = None

        # 차량 번호
        car_num = tree.xpath("//*[@id='CPOcontents']/div[1]/div[2]/div[1]/div[2]/div[2]/div/ol/li[10]/span[2]/text()")
        car_num = car_num[0].strip() if car_num else None
        
        # 최초등록일
        reg_day = tree.xpath("//*[@id='CPOcontents']/div[1]/div[2]/div[1]/div[2]/div[2]/div/ol/li[1]/span[2]/text()")
        reg_day = reg_day[0].strip() if reg_day else None

        # 조회수
        view = None

        # 연식
        year = tree.xpath("//*[@id='CPOcontents']/div[1]/div[2]/div[1]/div[2]/div[2]/div/ol/li[11]/span[2]/text()")
        year = year[0].strip() if year else None

        # 주행거리
        km = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[2]/div[2]/div/ol/li[2]/span[2]/text()')
        km = km[0].strip() if km else None

        # 연료
        fuel = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[2]/div[2]/div/ol/li[3]/span[2]/text()')
        fuel = fuel[0].strip() if fuel else None

        # 배기량
        cc = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[2]/div[2]/div/ol/li[4]/span[2]/text()')
        cc = cc[0].strip() if cc else None

        # 색상
        color = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[2]/div[2]/div/ol/li[5]/span[2]/text()')
        color = color[0].strip() if color else None

        # 보증정보
        ensure = tree.xpath('//*[@id="leftWarrantyTxt"]/text()')
        ensure = ensure[0].strip() if ensure else None

        # 설명글
        describe = None

        # 엔진형식
        engine = None

        # 연비
        effi = None

        # 최고출력, 최대토크, 차량중량
        other = None

        # 	선루프	
        sunroof = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[4]/div[2]/ol/li[9]')
        if sunroof and "off" in sunroof[0].get("class", ""):
            sunroof = "무"
        elif sunroof:
            sunroof = "유"
        else:
            sunroof = "무"
        
        # 파노라마선루프	
        pano_sunroof = None

        # 열선시트(앞좌석)	
        heat_front = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[4]/div[2]/ol/li[4]')
        if heat_front and "off" in heat_front[0].get("class", ""):
            heat_front = "무"
        elif heat_front:
            heat_front = "유"
        else:
            heat_front = "무"
        

        # 열선시트(뒷좌석)	
        heat_rear = None

        # 동승석에어백	
        airbag = None

        #후측방경보	
        alarm = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[4]/div[2]/ol/li[13]')
        if alarm and "off" in alarm[0].get("class", ""):
            alarm = "무"
        elif alarm:
            alarm = "유"
        else:
            alarm = "무"

        # 후방센서	
        sensor_rear = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[4]/div[2]/ol/li[12]')
        if sensor_rear and "off" in sensor_rear[0].get("class", ""):
            sensor_rear = "무"
        elif sensor_rear:
            sensor_rear = "유"
        else:
            sensor_rear = "무"

        # 전방센서	
        sensor_front = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[4]/div[2]/ol/li[16]')
        if sensor_front and "off" in sensor_front[0].get("class", ""):
            sensor_front = "무"
        elif sensor_front:
            sensor_front = "유"
        else:
            sensor_front = "무"
        
        # 후방카메라	
        camera_rear = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[4]/div[2]/ol/li[12]')
        if camera_rear and "off" in camera_rear[0].get("class", ""):
            camera_rear = "무"
        elif camera_rear:
            camera_rear = "유"
        else:
            camera_rear = "무"

        # 전방카메라	
        camera_front = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[4]/div[2]/ol/li[16]')
        if camera_front and "off" in camera_front[0].get("class", ""):
            camera_front = "무"
        elif camera_front:
            camera_front = "유"
        else:
            camera_front = "무"

        # 어라운드뷰
        around_view = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[4]/div[2]/ol/li[11]')
        if around_view and "off" in around_view[0].get("class", ""):
            around_view = "무"
        elif around_view:
            around_view = "유"
        else:
            around_view = "무"

        #  열선핸들
        heat_handle = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[4]/div[2]/ol/li[3]')
        if heat_handle and "off" in heat_handle[0].get("class", ""):
            heat_handle = "무"
        elif heat_handle:
            heat_handle = "유"
        else:
            heat_handle = "무"

        #오토라이트	
        auto_light = None

        # 크루즈컨트롤	
        cruise_control = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[4]/div[2]/ol/li[15]')
        if cruise_control and "off" in cruise_control[0].get("class", ""):
            cruise_control = "무"
        elif cruise_control:
            cruise_control = "유"
        else:
            cruise_control = "무"

        # 자동주차	
        auto_parking = None
        
        # 네비게이션(순정)	
        navi = None 

        # 네비게이션(비순정)	
        navi_tune = None

        # 보험처리수	
        ensure_num = tree.xpath('//*[@id="progress_history"]/div/p/text()')
        ensure_num = ensure_num[0].strip() if ensure_num else None
        
        # 소유자변경	
        owner_change = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[2]/div[3]/div[2]/ol/li[2]/span[2]/text()')
        owner_change = owner_change[0].strip() if owner_change else None
        
        # 전손 침수전손 침수분손
        loss = None
        
        # 도난	
        thief = None
        
        # 내차피해_횟수	
        my_car_damage = tree.xpath('//*[@id="CPOcontents"]/div[1]/div[2]/div[1]/div[2]/div[3]/div[2]/ol/li[1]/span[2]/text()')
        my_car_damage = my_car_damage[0].strip() if my_car_damage else None

        # 내차피해_금액	
        my_car_damage_amount = None
        
        # 타차가해_횟수	
        other_car_damage = None
        
        # 타차가해_금액	
        other_car_damage_amount = None
        
        # 판금	
        sheeting = None
        
        # 교환	
        change = None
        
        # 부식	
        corrosion = None
        
        # 사고침수유무	
        flooding = None
        
        # 불법구조변경	
        illegal = None
        
        # 브랜드
        brand = tree.xpath('//*[@id="p_scr01"]/div[1]/p/em/strong/text()')
        if brand:
            brand = brand[0].strip()
            if "현대" in brand:
                brand = "현대"
            elif "제네시스" in brand:
                brand = "제네시스"
            else:
                brand = None
        else:
            None

        return {
            "링크": link,
            "이름": name,
            "가격": price,
            "신차대비가격": new_car_percent,
            "신차가격": new_car_price,
            "차량번호": car_num,
            "최초등록일": reg_day,
            "조회수": view,
            "연식": year,
            "주행거리": km,
            "연료": fuel,
            "배기량": cc,
            "색상": color,
            "보증정보": ensure,
            "설명글": describe,
            "엔진형식": engine,
            "연비": effi,
            "최고출력": other,
            "최대토크": other,
            "차량중량": other,
            "선루프": sunroof,
            "파노라마선루프": pano_sunroof,
            "열선시트(앞좌석)": heat_front,
            "열선시트(뒷자석)": heat_rear,
            "동승석에어백": airbag,
            "후측방경보": alarm,
            "후방센서": sensor_rear,
            "전방센서": sensor_front,
            "후방카메라": camera_rear,
            "전방카메라": camera_front,
            "어라운드뷰": around_view,
            "열선핸들": heat_handle,
            "오토라이트": auto_light,
            "크루즈컨트롤": cruise_control,
            "자동주차": auto_parking,
            "네비게이션(순정)": navi,
            "네비게이션(비순정)": navi_tune,
            "보험처리수": ensure_num,
            "소유자변경": owner_change,
            "전손": loss,
            "침수전손": loss,
            "침수분손": loss,
            "도난": thief,
            "내차피해_횟수": my_car_damage,
            "내차피해_금액": my_car_damage_amount,
            "타차가해_횟수": other_car_damage,
            "타차가해_금액": other_car_damage_amount,
            "판금": sheeting,
            "교환": change,
            "부식": corrosion,
            "사고침수유무": flooding,
            "불법구조변경": illegal,
            "브랜드": brand
        }

    except Exception as e:
        print(f"{url} 데이터 파싱 오류: {e}")
        return None
    

"""여러 차량 정보를 동시에 크롤링"""
async def scrape_all_cars(urls):
    results = []

    # TCPConnector로 동시 요청 제한 해제
    connector = aiohttp.TCPConnector(limit=None)

    async with aiohttp.ClientSession(connector=connector) as session:
        tasks = [asyncio.create_task(fetch(session, url)) for url in urls]
        htmls = await asyncio.gather(*tasks)

        parse_tasks = [asyncio.create_task(parse_car_info(html, url)) for html, url in zip(htmls, urls)]
        car_infos = await asyncio.gather(*parse_tasks)

        results.extend([car for car in car_infos if car is not None])

    return results


async def main():
    print(f"{len(urls)}개의 차량 정보를 비동기 크롤링 시작.")
    car_data = await scrape_all_cars(urls)

    df_results = pd.DataFrame(car_data)
    df_results.to_csv("dataset/hyundai_certified_cars.csv", index=False, encoding="utf-8-sig")

    print("크롤링 완료, 파일 저장 완료.")


if __name__ == "__main__":
    asyncio.run(main())