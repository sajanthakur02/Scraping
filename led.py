import multiprocessing
import time
import mysql.connector
from urllib.parse import unquote
from concurrent.futures import ProcessPoolExecutor
from bs4 import BeautifulSoup
import requests
import re
import json
# ---------------------------------------------------------------
import logging
logging.basicConfig(format='%(asctime)s:%(name)s:%(message)s')
logger = logging.getLogger(__name__)
# ---------------------------------------------------------------

# function to scrap info
def scraper(url):
    domain = url.split("/")
    domain = "/".join(domain[:3])
    temp = {}
    headers = {
    'authority': 'www.google.com',
    'pragma': 'no-cache',
    'cache-control': 'no-cache',
    'dnt': '1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.66 Safari/537.36',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'sec-fetch-site': 'none',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-dest': 'document',
    'accept-language': 'en-US,en;q=0.9',
    'sec-gpc': '1',
    }
    params = (
        ('hl', 'en-US'),
        ('gl', 'us'),
    ) 
    page = requests.get(url, headers=headers, params=params)
    soup = BeautifulSoup(page.content, 'html.parser')

    mainDiv = soup.find('div', class_='columns')

    if mainDiv == None:
        return []
    else:
        # Product Link
        url = url.rstrip()

        vendor = url.split("/")
        vendor = vendor[2]
        if 'www' in vendor:
            vendor = vendor.split("www.")
            vendor = vendor[1].split(".co.in")
            vendor = vendor[0]
        else:
            vendor = vendor.split(".co.in")
            vendor = vendor[0]
        temp['vendor_name'] = vendor

        temp['product_link'] = url
        productNameDiv = mainDiv.find('span', class_='base')
        name = productNameDiv.get_text().strip()
        commareplace = name.replace("'","")
        productNameDiv = commareplace.replace(","," ")
        temp['product_name'] = productNameDiv
        featuresDiv = mainDiv.find("div", class_="product attribute description")
        featuresVal = featuresDiv.find("div", class_='value')
        if featuresVal == None:
            temp['product_features'] = ' '
        else:
            headings = " "
            # feature = featuresDiv.get_text().strip()
            for feature in featuresVal:
                if feature == "":
                    temp["product_features"] = ''
                else:
                    tempRow = feature.get_text().strip()
                    tempRow = tempRow.split("\n")
                    heading = '<p>'+" ".join(tempRow).replace("'", "").strip()+'</p>'
                    section = '<div>'+heading+'</div>'
                    headings += section
            htag = " "
            # feature = featuresDiv.get_text().strip()
            for feature in featuresVal.find_all('h4'):
                if feature == "":
                    temp["product_features"] = ''
                else:
                    tempRow = feature.get_text().strip()
                    tempRow = tempRow.split("\n")
                    heading = '<h4>'+" ".join(tempRow).replace("'", "").strip()+'</h4>'
                    section = '<div>'+heading+'</div>'
                    htag += section
            paragraphs = " "
            for feature in featuresVal.find_all('p'):
                if feature == "":
                    temp["product_features"] = ''
                else:
                    tempRow = feature.get_text().strip()
                    tempRow = tempRow.split("\n")
                    paragraph = '<p>'+" ".join(tempRow).replace("'", "").strip()+'</p>'
                    section = '<div>'+paragraph+'</div>'
                    paragraphs += section
            lists = " "
            for feature in featuresVal.find_all('ul'):
                if feature == "":
                    temp["product_features"] = ''
                else:
                    tempRow = feature.get_text().strip()
                    tempRow = tempRow.split("\n")
                    list_items = '<li>'+" ".join(tempRow).replace("'", "").strip()+'</li>'
                    section = '<div>'+list_items+'</div>'
                    lists += section
            temp["product_features"] = headings+htag+paragraphs+lists.replace("'","")

        imgDiv = mainDiv.find("div", class_="product media")
        imgScrap = imgDiv.find('script', {'type':'text/x-magento-init'})
        jsonStr = re.search(r'<script type=\"text/x-magento-init\">(.*)</script>', str(imgScrap), re.IGNORECASE | re.DOTALL).group(1)
        data = json.loads(jsonStr)
        image_datas = data['[data-gallery-role=gallery-placeholder]']['mage/gallery/gallery']['data']
        fullImg = ""
        for data_image in image_datas:
            img_Full = data_image['full']+','
            if img_Full == None:
                temp['product_image'] = ' '
            else:
                fullImg += img_Full
                
        imgData = fullImg[:-1]
        temp['product_image'] = imgData

        temp['product_pdfs'] = ' '
        temp['how_it_works'] = ' '
        temp['product_upc'] = ' '
        temp['product_brand'] = ' '
        productPriceDiv = mainDiv.find('div', class_='product-info-price')
        productBasePrice = productPriceDiv.find('span', class_='old-price')
        productPrice = productPriceDiv.find('span', class_='price')
        if productBasePrice == None:
            temp['product_base_price'] = productPrice.get_text().strip()
        else:
            productBasePrices = productBasePrice.get_text(" ",strip=True).split(" ")[0].replace("$", "").strip()
            temp['product_base_price'] = removeComma(productBasePrices)

        productNetPrice = productPriceDiv.find('span', class_='special-price')
        if productNetPrice == None:
            temp['product_net_price'] = ' '
        else:
            productNetPrices = productNetPrice.get_text(" ",strip=True).split(" ")[0].replace("$", "").strip()
            temp['product_net_price'] = removeComma(productNetPrices)
        productDiscount = productPriceDiv.find('span', class_='off-percent')
        if productDiscount == None:
            temp['product_discount'] = ' '
        else:
            temp['product_discount'] = productDiscount.get_text().strip().split("% Off")[0]

        productSku = productPriceDiv.find('div', class_='value')
        temp['product_sku'] = productSku.get_text().strip()

        productMpn = mainDiv.find("table", class_="data table additional-attributes")
        if productMpn == None:
            temp['product_mpn'] = ' '
        else:
            mpnProduct = productMpn.find("th", class_="col label")
            mpn =  mpnProduct.get_text().strip()
            if mpn in "Model Number":
                mpnProducts = productMpn.find("td", class_="col data")
                temp['product_mpn'] = mpnProducts.get_text().strip()
            else:
                temp['product_mpn'] = ' '

        spec = []
        productSpecifications = mainDiv.find('table', class_='data table additional-attributes')
        # productTbody = productSpecifications.find('tbody')
        for body in mainDiv.find_all('tr'):
            th = body.get_text().strip()
            t = th.split("\n")
            label = t[0].lower().replace(' ', '_')
            value = t[2]
            spec.append(label+"="+value)
        spec = "|".join(spec)           
        temp['additional_attributes'] = spec

        productStatus = mainDiv.find('div', class_='stock unavailable')
        if productStatus == None:
            temp['product_status'] = ' '
        else:
            temp['product_status'] = productStatus.get_text().strip()

        productShortDescription = mainDiv.find('div', class_='product attribute description')
        if productShortDescription == None:
            temp['product_description'] = ' '
        else:
            shortDesc = " "
            desc = productShortDescription.get_text().strip()
            para = '<p>'+desc+'</p>'
            section = '<div>'+para+'</div>'
            shortDesc += section
            temp['product_description'] = shortDesc.replace("\n", " ").replace("\xa0"," ").replace("'", "").strip()
        productDescription = mainDiv.find('div', class_='product attribute overview')
        if productDescription == None:
            temp['product_short_description'] = ' '
        else:
            description = " "
            for productDesc in productDescription.find_all('p'):
                if productDesc == None:
                    temp['product_short_description'] = ' '
                else:
                    tempRow = productDesc.get_text().strip()
                    if tempRow != '':
                        tempRow = tempRow.split("\n")
                        paragraph = '<p>'+" ".join(tempRow).replace("'", "").strip()+'</p>'
                        section = '<div>'+paragraph+'</div>'
                        description += section
            description1 = " "
            productDescriptions = productDescription.find('span', class_='completeDescription')
            if productDescriptions == None:
                temp['product_short_description'] = ' '
            else:
                for productDes in productDescriptions:
                    tempRow = productDes.get_text().strip()
                    if tempRow != '':
                        tempRow = tempRow.split("\n")
                        list_items = '<li>'+" ".join(tempRow).replace("'", "").strip()+'</li>'
                        section = '<div>'+list_items.replace("'", "")+'</div>'
                        description1 += section
            temp["product_short_description"] = description+description1.replace("'","")
        # print(temp)
        open("test2.txt","a",encoding="utf-8" ).write(str(temp))
        temp = makeInsertValues(temp['vendor_name'],temp['product_name'],temp['product_mpn'],temp['product_image'],temp["product_description"],temp['product_features'],temp['how_it_works'],temp['additional_attributes'],temp['product_pdfs'],temp['product_link'],temp['product_brand'],temp['product_sku'],temp['product_upc'],temp['product_base_price'],temp['product_discount'],temp['product_net_price'],temp['product_status'],temp['product_short_description'])

        return temp

def removeComma(value):
    strValue = str(value)
    if "," in strValue:
        newStrValue = strValue.replace(',', '')
        newStrValue = newStrValue.replace(' ', '') 
        # finalValue =  float(newStrValue)
        finalValue = newStrValue
    else:
        # finalValue =  float(strValue)
        finalValue = strValue
    return finalValue

# Fetching urls from file
def makeInsertValues(vendor, name, mpn, image, description, features, howit, additionalattr, pdfs, link, brand, sku, upc, base_price, discount, net_price, status, shortdscription):
    return f"('{vendor}', '{name}', '{mpn}', '{image}', '{description}', '{features}', '{howit}', '{additionalattr}','{pdfs}', '{link}', '{brand}', '{sku}', '{upc}', '{base_price}', '{discount}', '{net_price}', '{status}', '{shortdscription}')"

def insertIntoProducts(values):
    # print(values)
    try:
        conn = mysql.connector.connect(host='localhost', database='hotwater', user='root', password='')
        this = conn.cursor()

        insertQuery = "INSERT INTO  hotwater(vendor_name, product_name, product_mpn, product_image, product_description, product_features, how_it_works, additional_attributes, product_pdfs,product_link, product_brand, product_sku, upc, base_price, product_discount, net_price, product_status, product_short_description) VALUES "+values

        open("test.txt","w",encoding="utf-8" ).write(insertQuery)

        this.execute(insertQuery)
        conn.commit()
        print(this.rowcount, "Record inserted successfully")
    except mysql.connector.Error as error:
        print("Failed to insert record into table {}".format(error))
    finally:
        if conn.is_connected():
            this.close()
            conn.close()
            print("MySQL connection is closed")

def getUrls(fileName):
    with open(fileName) as file:
        file.seek(0)
        with ProcessPoolExecutor(max_workers=(multiprocessing.cpu_count() * 15)) as executor:
            tempValues = []
            for line in file:
                pass
                dataOut = executor.submit(scraper, line.strip())
                tempValues.append(dataOut)
            executor.shutdown(wait=True)
        return tempValues


if __name__ == '__main__':
    # starting process
    start = time.perf_counter()

    tempValues = [future.result() for future in getUrls('ledleighting.txt')]
    finish = time.perf_counter()
    logger.setLevel(logging.DEBUG)
    logger.info(f'Finished processes in {round(finish - start, 2)} second(s)')
     
    insertValues = ", ".join(tempValues)
    open("test2.txt","w",encoding="utf-8" ).write(str(tempValues))

    # insertIntoProducts(insertValues)