import threading
import asyncio
import urllib.parse as up
import json

from playwright.async_api import async_playwright


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

async def main(brands, nums):    
    DEEP_FILTER = 10
    DEEP_ANALOG = 10
    ANALOG = True
    IS_BIGGER = None #True - больше False - меньше None - не указано
    DATE = 5
    for brand, num in zip(brands, nums):
        url = f"https://emex.ru/api/search/search?make={create_params_for_url(brand)}&detailNum={num}&locationId=38760&showAll=true&longitude=37.8613&latitude=55.7434"
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False, proxy={"server": "http://193.58.168.161:1050", "username": "LorNNF", "password": "fr4B7cGdyS"})
            page = await browser.new_page()

            try:
                await page.goto(url, timeout=3000)
            except:
                await page.goto(url, timeout=3000)

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
                await page.goto(f"https://emex.ru/api/search/rating?offerKey={best_data[0]}", timeout=3000)
            except:
                await page.goto(f"https://emex.ru/api/search/rating?offerKey={best_data[0]}", timeout=3000)

            pre_with_logo = await (await page.query_selector("pre")).text_content()
            response_with_logo = dict(json.loads(pre_with_logo))
            price_logo = response_with_logo["priceLogo"] 

            result = [price_logo, *best_data[1:]]
            print(result)

brands = ["peugeot+%2F+citroen"]
nums = ["00004254A2"]

asyncio.run(main(brands, nums))