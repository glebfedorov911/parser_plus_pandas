import threading
import asyncio
import urllib.parse as up
import json
import time

from playwright.async_api import async_playwright

from math import ceil

#тест гит на ноуте
def split_file_for_thr(num: int, url: list) -> list[list]:
    '''
    num - число потоков # например 4
    url - список с url => [...] # 16 штук
    list[list] - список со списками url => [[...]] # 4 по 4 
    '''
    new_url = []
    step = ceil(len(url)/num)
    for i in range(0, len(url), step):
        if i+step > len(url)-1:
            new_url.append(url[i:])
        else:
            new_url.append(url[i:i+step])

    return new_url

def create_params_for_url(param: str):
    if "---" in param:
        param = param.replace("---", "+%2F+")
        return param
    if " / " in param:
        param = param.replace(" / ", "+%2F+")
        return param
    return up.quote(param)

def quick_sort(arr: list, index: int):
    '''
    Алгоритм быстрой сортировки
    arr - массив с массивами, которые будут сортироваться
    index - номер элемента (с 0) по которому мы с сортируем нашима массивы
    '''
    if len(arr) <= 1:
        return arr
    else:
        pivot = arr[len(arr) // 2][index]
        left = [x for x in arr if x[index] < pivot]
        middle = [x for x in arr if x[index] == pivot]
        right = [x for x in arr if x[index] > pivot]
        return quick_sort(left, index) + middle + quick_sort(right, index)

PROXY_LIST = [
    ["http://46.8.16.194:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://46.8.22.63:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://109.248.14.248:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://2.59.50.242:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://94.158.190.152:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://188.130.129.128:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://31.40.203.252:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://45.15.73.112:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://46.8.157.208:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://188.130.128.166:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://194.156.97.212:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://194.156.123.115:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://109.248.166.189:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://91.188.244.80:1050", "LorNNF", "fr4B7cGdyS"],
    ["http://193.58.168.161:1050", "LorNNF", "fr4B7cGdyS"],
]

len_proxy_list = len(PROXY_LIST)
ban_list = set()

async def main(brands, nums):   
    global PROXY_LIST, ban_list, len_proxy_list

    DEEP_FILTER = 50
    DEEP_ANALOG = 50
    ANALOG = False
    IS_BIGGER = True #True - больше False - меньше None - не указано
    DATE = 5
    LOGO = "HXAW" #HXAW - пример лого None - Без лого
    
    if PROXY_LIST != []:
        proxy = PROXY_LIST.pop(0)
    else:
        proxy = ["http://test:8888", "user1", "pass1"]
    for brand, num in zip(brands, nums):
        if len_proxy_list == len(ban_list):
            print("Закончились все прокси")
            break

        url = f"https://emex.ru/api/search/search?make={create_params_for_url(brand)}&detailNum={num}&locationId=38760&showAll=true&longitude=37.8613&latitude=55.7434"
        async with async_playwright() as p:
            try:
                browser = await p.chromium.launch(headless=False, proxy={"server": proxy[0], "username": proxy[1], "password": proxy[2]})
                page = await browser.new_page()

                try:
                    await page.goto(url, timeout=1500)
                except:
                    await page.goto(url, timeout=1500)

                pre = await (await page.query_selector("pre")).text_content()
                response = dict(json.loads(pre))
                originals = []

                if IS_BIGGER is None:
                    if "originals" in response["searchResult"]:
                        originals += [[goods["offerKey"], int(str(goods["delivery"]["value"]).replace("Завтра", "1")), goods["displayPrice"]["value"], goods["data"]["maxQuantity"]["value"]] for orig in response["searchResult"]["originals"] for goods in orig["offers"]]
                    else:
                        print("Товара нет в наличие")
                        continue

                    if "replacements" in response["searchResult"]:
                        originals += [[goods["offerKey"], int(str(goods["delivery"]["value"]).replace("Завтра", "1")), goods["displayPrice"]["value"], goods["data"]["maxQuantity"]["value"]] for repl in response["searchResult"]["replacements"] for goods in repl["offers"]]

                    if ANALOG and "analogs" in response["searchResult"]:
                        originals += [[goods["offerKey"], int(str(goods["delivery"]["value"]).replace("Завтра", "1")), goods["displayPrice"]["value"], goods["data"]["maxQuantity"]["value"]] for anal in response["searchResult"]["analogs"][:DEEP_ANALOG] for goods in anal["offers"]]
                else:
                    if "originals" in response["searchResult"]:
                        if IS_BIGGER:
                            originals += [[goods["offerKey"], int(str(goods["delivery"]["value"]).replace("Завтра", "1")), goods["displayPrice"]["value"], goods["data"]["maxQuantity"]["value"]] if int(str(goods["delivery"]["value"]).replace("Завтра", "1")) >= DATE else False for orig in response["searchResult"]["originals"] for goods in orig["offers"]]
                        else:
                            originals += [[goods["offerKey"], int(str(goods["delivery"]["value"]).replace("Завтра", "1")), goods["displayPrice"]["value"], goods["data"]["maxQuantity"]["value"]] if int(str(goods["delivery"]["value"]).replace("Завтра", "1")) <= DATE else False for orig in response["searchResult"]["originals"] for goods in orig["offers"]]
                    else:
                        print("Товара нет в наличие")
                        continue

                    if "replacements" in response["searchResult"]:
                        if IS_BIGGER:
                            originals += [[goods["offerKey"], int(str(goods["delivery"]["value"]).replace("Завтра", "1")), goods["displayPrice"]["value"], goods["data"]["maxQuantity"]["value"]] if int(str(goods["delivery"]["value"]).replace("Завтра", "1")) >= DATE else False for orig in response["searchResult"]["replacements"] for goods in orig["offers"]]
                        else:
                            originals += [[goods["offerKey"], int(str(goods["delivery"]["value"]).replace("Завтра", "1")), goods["displayPrice"]["value"], goods["data"]["maxQuantity"]["value"]] if int(str(goods["delivery"]["value"]).replace("Завтра", "1")) <= DATE else False for orig in response["searchResult"]["replacements"] for goods in orig["offers"]]

                    if ANALOG and "analogs" in response["searchResult"]:
                        if IS_BIGGER:
                            originals += [[goods["offerKey"], int(str(goods["delivery"]["value"]).replace("Завтра", "1")), goods["displayPrice"]["value"], goods["data"]["maxQuantity"]["value"]] if int(str(goods["delivery"]["value"]).replace("Завтра", "1")) >= DATE else False for orig in response["searchResult"]["analogs"][:DEEP_ANALOG] for goods in orig["offers"]]
                        else:
                            originals += [[goods["offerKey"], int(str(goods["delivery"]["value"]).replace("Завтра", "1")), goods["displayPrice"]["value"], goods["data"]["maxQuantity"]["value"]] if int(str(goods["delivery"]["value"]).replace("Завтра", "1")) <= DATE else False for orig in response["searchResult"]["analogs"][:DEEP_ANALOG] for goods in orig["offers"]]
                    
                    originals = [data for data in originals if data]

                sorted_data_by_date = quick_sort(originals, 1)
                cut_data_by_date = sorted_data_by_date[:len(sorted_data_by_date)//2+1]

                sorted_data_by_availability = quick_sort(cut_data_by_date, 3)
                cut_data_by_availability = sorted_data_by_availability[-DEEP_FILTER:]

                best_data = min(cut_data_by_availability, key=lambda x: x[2])

                try:
                    await page.goto(f"https://emex.ru/api/search/rating?offerKey={best_data[0]}", timeout=1500)
                except:
                    await page.goto(f"https://emex.ru/api/search/rating?offerKey={best_data[0]}", timeout=1500)

                pre_with_logo = await (await page.query_selector("pre")).text_content()
                response_with_logo = dict(json.loads(pre_with_logo))
                price_logo = response_with_logo["priceLogo"] 

                result = [price_logo, *best_data[1:]]
                print(num, result)
                
                if LOGO:
                    best_data = None
                    sorted_by_price = quick_sort(originals, 2)
                    for data in sorted_by_price:
                        try:
                            await page.goto(f"https://emex.ru/api/search/rating?offerKey={data[0]}", timeout=1500)
                        except:
                            sorted_by_price.append(data)
                            continue

                        pre_with_logo = await (await page.query_selector("pre")).text_content()
                        response_with_logo = dict(json.loads(pre_with_logo))
                        price_logo = response_with_logo["priceLogo"] 

                        data[0] = price_logo
                        if price_logo == LOGO:
                            best_data = data
                            break

                    if best_data:
                        print(num, best_data)
                    else:
                        print(num, "Нет такого лого среди оригиналов")
            except:
                brands.append(brand)
                nums.append(num)
                if proxy != ["http://test:8888", "user1", "pass1"]:
                    ban_list.add("@".join(proxy))
                if PROXY_LIST != []:
                    proxy = PROXY_LIST.pop(0)
                else:
                    proxy = ["http://test:8888", "user1", "pass1"]
    PROXY_LIST.append(proxy)
        
def run(brands, nums):
    asyncio.run(main(brands, nums))


start = time.perf_counter()
brands = ["peugeot---citroen", "ГАЗ", "peugeot---citroen", "peugeot---citroen", "peugeot---citroen", "peugeot---citroen", "Mahle---Knecht", "VAG", "Autocomponent"] * 2
nums = ["82026", "6270000290", "00008120T7", "00006426YN", "00004254A2", "362312", "02943N0", "016409399B", "01М21С9"] * 2 

brands_split = split_file_for_thr(4, brands) # 4 - количество потоков
nums_split = split_file_for_thr(4, nums)

threadings = []
for i in range(len(brands_split)):
    thread = threading.Thread(target=run, args=(brands_split[i], nums_split[i]), name=f"thr-{i}")
    thread.start()
    threadings.append(thread)

for thread in threadings:
    thread.join()

print("Бан лист:", ban_list)
print(time.perf_counter()-start)


# -=-=-=-=-
    # DEEP_FILTER = 50
    # DEEP_ANALOG = 50
    # ANALOG = False
    # IS_BIGGER = True #True - больше False - меньше None - не указано
    # DATE = 5
    # LOGO = None #HXAW - пример лого None - Без лого
    #
    # 18 строк за 12.4 секунды
# -=-=-=-=-
    # DEEP_FILTER = 50
    # DEEP_ANALOG = 50
    # ANALOG = True
    # IS_BIGGER = True #True - больше False - меньше None - не указано
    # DATE = 5
    # LOGO = None #HXAW - пример лого None - Без лого
    # 
    #  18 строк за 11.0 секунд
# -=-=-=-=-
    # DEEP_FILTER = 50
    # DEEP_ANALOG = 50
    # ANALOG = False
    # IS_BIGGER = True #True - больше False - меньше None - не указано
    # DATE = 5
    # LOGO = "HXAW" #HXAW - пример лого None - Без лого
    #
    # 18 строк за 145.4 секунды
# -=-=-=-=-
    # DEEP_FILTER = 50
    # DEEP_ANALOG = 50
    # ANALOG = False
    # IS_BIGGER = True #True - больше False - меньше None - не указано
    # DATE = 5
    # LOGO = "HXAW" #HXAW - пример лого None - Без лого
    #
    # измененный код 18 строк за 84.67 секунды
# -=-=-=-=-
    # Среднее 1) 0.70 2) 0.61 3) 8.10 4) 4.70