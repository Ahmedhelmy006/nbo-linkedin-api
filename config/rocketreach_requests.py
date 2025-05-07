import requests

def get_lkd_profile_devloper_nbo(email_address):
    url = "https://rocketreach.co/v2/services/search/person"

    headers = {
        "authority": "rocketreach.co",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json;charset=UTF-8",
        "cookie": '__uc="{\\"e_id\\":43693649\\054\\"e_type\\":1\\054\\"e_view\\":4\\054\\"e_locale\\":\\"en-US\\"}"; _cfuvid=XlF3vyKM4YUlfsOPQR9mmrIO7JZIlTGt066m.5rs3UY-1744637381047-0.0.1.1-604800000; __hstc=94151554.958dfc4816425c35cc68dcc5b7386c1a.1744637396151.1744637396151.1744637396151.1; hubspotutk=958dfc4816425c35cc68dcc5b7386c1a; __hssrc=1; jh=1744637401; src=200; cj2=L2FuZ2VsYS1jb253YXktZW1haWxfNDM2OTM2NDk; _ga=GA1.1.1160054423.1744637402; _clck=19d5pxw|2|fv2|0|1930; validation_token=mbo7UhCXIwLKQoUNrmgLU0g11SALgJqx; sessionid-20191028=5u215hzlyco0hd40n1zj1xghjmnxwj8u; sessionid-20191028=5u215hzlyco0hd40n1zj1xghjmnxwj8u; __ssid=98a9c06847317d2061345709df8f162; datadome=f7Rs33ZX_9pFs1KMc8lrsmz_f3K6ttwAokSBrbYs~xlsXHEzGhoXnlHM4VMjnqChvX80js~ucuM_uuvcDqg2GLOmirFnWjTnfY8d5iYm47wjbtFcRl40Olx5AW8Qfcnb; _cq_duid=1.1744637565.YPi5AOqWaFPYxS0O; _cq_suid=1.1744637565.56YC6X3lYhgoaBjh; OptanonConsent=isGpcEnabled=0&datestamp=Mon+Apr+14+2025+15%3A43%3A47+GMT%2B0200+(Eastern+European+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=44efe4db-b54a-47df-a2ca-9c0b464d99a2&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0002%3A1%2CC0001%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false; _rdt_uuid=1744637415176.2a856933-1567-46e5-82a7-b81ac2f8c0d1; _uetsid=9abf7230193411f0833cc5982d0ab5a0; _uetvid=9abf7cf0193411f0aa718ff4177f63f5; _ga_FB8KKHJC7E=GS1.1.1744642393.2.1.1744642559.51.0.0; _gcl_au=1.1.1209370702.1744637402.859230519.1744642394.1744642559',
        "origin": "https://rocketreach.co",
        "priority": "u=1, i",
        "referer": f"https://rocketreach.co/person?start=1&pageSize=10&keyword={email_address}",
        "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "x-csrftoken": "o5TKnPewxvXI5k9fwqjtmmJEIoVJWjDKA67H7WGj5RyiLyTSNCp46cPvz6lk2ST7",
        "x-rr-for": "50b95946541a64ae1986e71786539025",
        "x-source-page": "/person"
    }

    payload = {
        "keyword": f"{email_address}",
        "start": 1,
        "pageSize": 10
    }
    linkedin_url = None

    response = requests.post(url, headers=headers, json=payload,)
    response_data = response.json()
    if response_data.get('people') and len(response_data['people']) > 0:
        first_person = response_data['people'][0]
        if 'links' in first_person and 'linkedin' in first_person['links']:
            linkedin_url = first_person['links']['linkedin']
            print(linkedin_url)
        else:
            print("No LinkedIn URL found in the first person's data.")
            return None
    else:
        print("No people data found in the response.")

        print(f"Status Code: {response.status_code}")
        return(linkedin_url)
    

def get_lkd_profile_muhammad_helmey_006(email_address):

    url = "https://rocketreach.co/v2/services/search/person"

    headers = {
        "authority": "rocketreach.co",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://rocketreach.co",
        "priority": "u=1, i",
        "referer": f"https://rocketreach.co/person?start=1&pageSize=10&contact_info={email_address}",
        "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "x-csrftoken": "5zqe2ymBlRfCvqzemvMaj9kNdsVDphQRZkUAiZagWO8rNKLnNoDf1iryZc8GmM16",
        "x-rr-for": "085965807e718cf5d1d6ffb757e0ddde",
        "x-source-page": "/person"
    }

    cookies = {
        "_cfuvid": "7dhA2iolEeJUgtnEMOLxLaTfndx0iN47XTfuV1FX4M8-1744998817899-0.0.1.1-604800000",
        "jh": "1744998823",
        "src": "200",
        "cj2": "L3BlcnNvbg",
        "_clck": "1pr0ljf|2|fv6|0|1934",
        "__hstc": "94151554.887b07b49252b233cfdf303441b7e915.1744998824030.1744998824030.1744998824030.1",
        "hubspotutk": "887b07b49252b233cfdf303441b7e915",
        "__hssrc": "1",
        "_gcl_au": "1.1.837204582.1744998824",
        "_ga": "GA1.1.1638730989.1744998824",
        "__ssid": "33a43bc0517623936dd74378cd530cd",
        "viewedOuibounceModal": "true",
        "validation_token": "4VEwqBYPL73ZsumjB31fSjhVWUnd7Flp",
        "sessionid-20191028": "uf6kk1g4ofwcpblgzugx4nyo54emubfo",
        "oauthSourcePage": "unknown",
        "_ga_FB8KKHJC7E": "GS1.1.1744998823.1.1.1744998966.56.0.0",
        "OptanonConsent": "isGpcEnabled=0&datestamp=Fri+Apr+18+2025+19%3A56%3A06+GMT%2B0200+(Eastern+European+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=b94ca905-490c-4c3b-ba02-35378d7aa326&interactionCount=0&isAnonUser=1&landingPath=NotLandingPage&groups=C0002%3A1%2CC0001%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false",
        "_clsk": "1lddnpb|1744998966371|9|0|i.clarity.ms/collect",
        "_rdt_uuid": "1744998823646.2069b24a-be2d-43a4-ae11-43479df799f6",
        "_uetsid": "12474f401c7e11f0be9e47c64961e272|5gu2n5|2|fv6|0|1934",
        "_uetvid": "12473cf01c7e11f0bf7a01b18fb079c3|1iamo3m|1744998884060|2|1|bat.bing.com/p/insights/c/i",
        "__hssc": "94151554.6.1744998824030"
    }

    payload = {
        "keyword": f"{email_address}",  # Assuming same payload as previous examples
        "start": 1,
        "pageSize": 10
    }
    linkedin_url = None
    response = requests.post(url, headers=headers, json=payload, cookies=cookies)
    print(response.status_code)
    response_data = response.json()
    if response_data.get('people') and len(response_data['people']) > 0:
        first_person = response_data['people'][0]
        if 'links' in first_person and 'linkedin' in first_person['links']:
            linkedin_url = first_person['links']['linkedin']
            return(linkedin_url)
        else:
            print("No LinkedIn URL found in the first person's data.")
            return None
    else:
        print("No people data found in the response.")

        print(f"Status Code: {response.status_code}")
        return(linkedin_url)

def get_lkd_profile_ahmed_helmey_006(email_address):

    url = "https://rocketreach.co/v2/services/search/person"

    headers = {
        "authority": "rocketreach.co",
        "method": "POST",
        "path": "/v2/services/search/person",
        "scheme": "https",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-GB,en;q=0.9,de-DE;q=0.8,de;q=0.7,ar-AE;q=0.6,ar;q=0.5,en-US;q=0.4",
        "content-type": "application/json;charset=UTF-8",
        "dnt": "1",
        "origin": "https://rocketreach.co",
        "priority": "u=1, i",
        "referer": f"https://rocketreach.co/person?start=1&pageSize=10&contact_info={email_address}",
        "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "sec-gpc": "1",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "x-csrftoken": "M70WEZU8dac0yvkMOrv6ESfEJlSdJbCl4pueAIf93sCXhk0Vk8GC266hn0PJglva",
        "x-rr-for": "3d647baa374aacb9717d668ccd7bc882",
        "x-source-page": "/person"
    }

    cookies = {
        "jh": "1744638037",
        "src": "200",
        "cj2": "L3BlcnNvbg",
        "_cq_duid": "1.1744638037.lwzEr48ZWwoGTxyx",
        "_gcl_au": "1.1.713168854.1744638038",
        "_ga": "GA1.1.1937525704.1744638038",
        "hubspotutk": "0dd90d28d9f544656968967a1c09c87f",
        "__ssid": "f9eabca7abb252b1af1ebf63e6cd097",
        "_cfuvid": "1Ph.PqMjQyxeQCFfm6CgO1Ibnxcy2dg7u4yYnXfUreU-1744996871594-0.0.1.1-604800000",
        "__hstc": "94151554.0dd90d28d9f544656968967a1c09c87f.1744638056311.1744638056311.1744996877344.2",
        "__hssrc": "1",
        "viewedOuibounceModal": "true",
        "validation_token": "ssEs6Tvb0sA7TZQjGRlGyo1NOP7GHk3Z",
        "sessionid-20191028": "blalrrxqigbyrsu4t82jx8618lggm724",
        "oauthSourcePage": "unknown",
        "_rdt_uuid": "1744638038377.14c1b557-a890-4cc2-ab4a-5bffa8ea7240",
        "_ga_FB8KKHJC7E": "GS1.1.1744996877.3.1.1744997270.0.0.0",
        "__hssc": "94151554.4.1744996877344",
        "OptanonConsent": "isGpcEnabled=1&datestamp=Fri+Apr+18+2025+19%3A27%3A51+GMT%2B0200+(Eastern+European+Standard+Time)&version=202409.1.0&browserGpcFlag=1&isIABGlobal=false&hosts=&consentId=78d67130-f5ac-4cf5-bc38-e6934fa29b79&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0002%3A1%2CC0001%3A1%2CC0003%3A1%2CC0004%3A0&AwaitingReconsent=false"
    }

    payload = {
        "keyword": f"{email_address}",  # Assuming same payload as previous examples
        "start": 1,
        "pageSize": 10
    }


    linkedin_url = None
    
    response = requests.post(url, headers=headers, json=payload,cookies=cookies)
    response_data = response.json()
    if response_data.get('people') and len(response_data['people']) > 0:
        first_person = response_data['people'][0]
        if 'links' in first_person and 'linkedin' in first_person['links']:
            linkedin_url = first_person['links']['linkedin']
            print(linkedin_url)
        else:
            print("No LinkedIn URL found in the first person's data.")
            return None
    else:
        print("No people data found in the response.")
        print(f"Status Code: {response.status_code}")
        return None  # Return None instead of an undefined variable

    return linkedin_url


def get_lkd_profile_ahmed_helmey_009(email_address):


    url = "https://rocketreach.co/v2/services/search/person"

    headers = {
        "authority": "rocketreach.co",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://rocketreach.co",
        "priority": "u=1, i",
        "referer": f"https://rocketreach.co/person?start=1&pageSize=10&contact_info={email_address}",
        "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "x-csrftoken": "EZEfh5F1dXewlNxBxYhIkrpTlDtAgnajXifOBZ9GImTBilwdKSF8YDoMqe6ibUIi",
        "x-rr-for": "085965807e718cf5d1d6ffb757e0ddde",
        "x-source-page": "/person"
    }

    cookies = {
        "src": "300",
        "cj2": "Lw",
        "jh": "1740960000",
        "_cfuvid": "mwW4GOBxZ.tboLpDoZ7.VEiRf85qkGOeiMrWoVSpUjk-1744997830688-0.0.1.1-604800000",
        "_clck": "gzo7aq|2|fv6|0|1934",
        "_gcl_au": "1.1.767696385.1744997835",
        "_ga": "GA1.1.1271082271.1744997835",
        "__ssid": "b352def11c62b1a0e93b8845af702d9",
        "validation_token": "ttLJu4EPFzPf7I9Mn4yAOm93fLNS5HI9",
        "sessionid-20191028": "9r4bgdpn8pzt6y4qgu5icrnidv1o71ff",
        "oauthSourcePage": "unknown",
        "__hstc": "94151554.353b7dc0cacb8f8bd537222e580d5228.1744997930871.1744997930871.1744997930871.1",
        "hubspotutk": "353b7dc0cacb8f8bd537222e580d5228",
        "__hssrc": "1",
        "_ga_FB8KKHJC7E": "GS1.1.1744997834.1.1.1744998056.2.0.0",
        "_rdt_uuid": "1744997835017.e949c614-84c7-426e-aa8e-4575a0845364",
        "OptanonConsent": "isGpcEnabled=0&datestamp=Fri+Apr+18+2025+19%3A40%3A57+GMT%2B0200+(Eastern+European+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=f226864e-c765-460f-89ba-87f888881567&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0002%3A1%2CC0001%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false",
        "_uetsid": "c4e5ec901c7b11f085a565ecf1368dc3|prft9l|2|fv6|0|1934",
        "_uetvid": "c4e616201c7b11f0969c3588357ec864|169rhaz|1744997938781|2|1|bat.bing.com/p/insights/c/i",
        "_clsk": "1n8q73e|1744998057103|13|0|i.clarity.ms/collect",
        "__hssc": "94151554.7.1744997930871"
    }

    payload = {
        "keyword": f"{email_address}",  # Assuming same payload as previous examples
        "start": 1,
        "pageSize": 10
    }

    response = requests.post(
        url,
        headers=headers,
        cookies=cookies,
        json=payload
    )


    linkedin_url = None
    
    response = requests.post(url, headers=headers, json=payload,cookies=cookies)
    response_data = response.json()
    if response_data.get('people') and len(response_data['people']) > 0:
        first_person = response_data['people'][0]
        if 'links' in first_person and 'linkedin' in first_person['links']:
            linkedin_url = first_person['links']['linkedin']
            print(linkedin_url)
        else:
            print("No LinkedIn URL found in the first person's data.")
            return None
    else:
        print("No people data found in the response.")
        print(f"Status Code: {response.status_code}")
        return None  # Return None instead of an undefined variable

    return linkedin_url

def get_lkd_profile_ichbin(email_address):

    url = "https://rocketreach.co/v2/services/search/person"

    headers = {
        "authority": "rocketreach.co",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://rocketreach.co",
        "priority": "u=1, i",
        "referer": f"https://rocketreach.co/person?start=1&pageSize=10&contact_info={email_address}",
        "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "x-csrftoken": "IB6o1cWlKPrJ4Xftk44uOe0tDgU5IzKKGOY7D5nJmWE1OBGS80o2iInEbQv2YXTH",
        "x-rr-for": "085965807e718cf5d1d6ffb757e0ddde",
        "x-source-page": "/person"
    }

    cookies = {
        "_cfuvid": "rSrl0_G0tyi6e10pf.h.pX2T5AAr6yvFMIuk.GWDgmw-1745000308244-0.0.1.1-604800000",
        "__hstc": "94151554.1d08ec1d8132bb9fe6d4a960568a6333.1745000313593.1745000313593.1745000313593.1",
        "hubspotutk": "1d08ec1d8132bb9fe6d4a960568a6333",
        "__hssrc": "1",
        "jh": "1745000313",
        "src": "200",
        "cj2": "L3BlcnNvbg",
        "_clck": "eu3b6p|2|fv6|0|1934",
        "_gcl_au": "1.1.1396774414.1745000314",
        "_ga": "GA1.1.962974972.1745000314",
        "viewedOuibounceModal": "true",
        "validation_token": "8n2TM3ByMhnsUOBzY6uIEExlIKL7qyj7",
        "sessionid-20191028": "lkzwfesaksfab3e5bibbu2ys9jib96j7",
        "oauthSourcePage": "unknown",
        "__ssid": "19826a80b12fd422cb8a1cac346776e",
        "_ga_FB8KKHJC7E": "GS1.1.1745000313.1.1.1745000416.17.0.0",
        "_rdt_uuid": "1745000313974.2a41188c-b392-4ad6-9fe3-cdf9b218517e",
        "OptanonConsent": "isGpcEnabled=0&datestamp=Fri+Apr+18+2025+20%3A20%3A16+GMT%2B0200+(Eastern+European+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=26683276-10f3-4549-bd18-6857aa56bb6f&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0002%3A1%2CC0001%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false",
        "_uetsid": "8a83e2501c8111f088287d3f20d0e608|uhntn6|2|fv6|0|1934",
        "_uetvid": "8a83e3c01c8111f0bd8803135504e2bb|1ttx2qu|1745000404980|3|1|bat.bing.com/p/insights/c/i",
        "_clsk": "1pj16kh|1745000416959|6|0|i.clarity.ms/collect",
        "__hssc": "94151554.4.1745000313593"
    }

    payload = {
        "keyword": f"{email_address}",  # Same payload structure as previous examples
        "start": 1,
        "pageSize": 10
    }

    linkedin_url = None
    
    response = requests.post(url, headers=headers, json=payload, cookies=cookies)
    response_data = response.json()
    if response_data.get('people') and len(response_data['people']) > 0:
        first_person = response_data['people'][0]
        if 'links' in first_person and 'linkedin' in first_person['links']:
            linkedin_url = first_person['links']['linkedin']
            print(linkedin_url)
        else:
            print("No LinkedIn URL found in the first person's data.")
            return None
    else:
        print("No people data found in the response.")
        print(f"Status Code: {response.status_code}")
        return None  # Return None instead of an undefined variable

    return linkedin_url


def get_lkd_profile_ahmed_modelwiz(email_address):

    url = "https://rocketreach.co/v2/services/search/person"

    headers = {
        "authority": "rocketreach.co",
        "accept": "application/json, text/plain, */*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.9",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://rocketreach.co",
        "priority": "u=1, i",
        "referer": f"https://rocketreach.co/person?start=1&pageSize=10&contact_info={email_address}",
        "sec-ch-ua": '"Google Chrome";v="135", "Not-A.Brand";v="8", "Chromium";v="135"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Windows"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36",
        "x-csrftoken": "oMairk2Kfv9iS5ieoepv0XFYfhvrX5QZOxIWT1dugSqPRjNhj4oMbynA2cVZpRkT",
        "x-rr-for": "50b95946541a64ae1986e71786539025",
        "x-source-page": "/person"
    }

    cookies = {
        "src": "300",
        "cj2": "Lw",
        "jh": "1740960000",
        "_cfuvid": "aGSUj77YDoTNaDeSLeu4ayAaQS6TvnLEuvGQeDn89kE-1744998184836-0.0.1.1-604800000",
        "_clck": "1d4fqq|2|fv6|0|1934",
        "_gcl_au": "1.1.1403154614.1744998190",
        "_ga": "GA1.1.1255138327.1744998190",
        "__ssid": "d12e7a90d97bc4c288f3dab18762fc2",
        "validation_token": "AVIOCRlUbxrH9oFd509rlLSMX5AICWE4",
        "sessionid-20191028": "csit71idw9bhmccy8pnr9uceg3ypj3lq",
        "oauthSourcePage": "unknown",
        "__hstc": "94151554.f77fd9ae04d0d475eb8b6270fac08e4b.1744998759817.1744998759817.1744998759817.1",
        "hubspotutk": "f77fd9ae04d0d475eb8b6270fac08e4b",
        "__hssrc": "1",
        "_ga_FB8KKHJC7E": "GS1.1.1744998189.1.1.1744998797.21.0.0",
        "_rdt_uuid": "1744998189304.63c42e37-da35-41c8-a7fb-045cfb153ba9",
        "OptanonConsent": "isGpcEnabled=0&datestamp=Fri+Apr+18+2025+19%3A53%3A17+GMT%2B0200+(Eastern+European+Standard+Time)&version=202409.1.0&browserGpcFlag=0&isIABGlobal=false&hosts=&consentId=e4de60ce-09cf-46e8-98d5-18af532bb49f&interactionCount=1&isAnonUser=1&landingPath=NotLandingPage&groups=C0002%3A1%2CC0001%3A1%2CC0003%3A1%2CC0004%3A1&AwaitingReconsent=false",
        "_uetsid": "981000201c7c11f0aa85b506e653a50c",
        "_uetvid": "981011401c7c11f0b2920d23ea36aa87",
        "_clsk": "vs57dv|1744998797868|5|1|i.clarity.ms/collect",
        "__hssc": "94151554.3.1744998759817"
    }

    payload = {
        "keyword": f"{email_address}",  # Assuming same payload as previous example
        "start": 1,
        "pageSize": 10
    }


    linkedin_url = None
    
    response = requests.post(url, headers=headers, json=payload,cookies=cookies)
    response_data = response.json()
    if response_data.get('people') and len(response_data['people']) > 0:
        first_person = response_data['people'][0]
        if 'links' in first_person and 'linkedin' in first_person['links']:
            linkedin_url = first_person['links']['linkedin']
            print(linkedin_url)
        else:
            print("No LinkedIn URL found in the first person's data.")
            return None
    else:
        print("No people data found in the response.")
        print(f"Status Code: {response.status_code}")
        return None  # Return None instead of an undefined variable

    return linkedin_url