from typing import List, Dict
import aiometer
import asyncio
import random
import httpx
import json
import os
import re
from bs4 import BeautifulSoup
from tqdm import tqdm

USER_AGENTS = [
    "Mozilla/5.0 (Linux; Android 8.0.0; Pixel 2 Build/OPD1.170811.002; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/59.0.3071.125 Mobile Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1",
    "Mozilla/5.0 (X11; CrOS x86_64 8172.45.0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.64 Safari/537.36",
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/112.0",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/13.0.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Windows; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8",
    "Mozilla/5.0 (Windows NT 10.0; Windows; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Windows; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.114 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.5060.53 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/42.0.2311.135 Safari/537.36 Edge/12.246",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.111 Safari/537.36",
]

HEADERS = {
    "Host": "www.bricklink.com",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}

BRICK_PART_CLASS = "pciinvItemRow"

MAX_SET_PAGE_NUMBER = 372


# Create an error for going over the rate limit
class RateLimitError(Exception):
    pass


def quota_is_exceeded(page_text: str) -> bool:
    soup = BeautifulSoup(page_text, "html.parser")
    error_title = soup.find("span", {"id": "blErrorTitle"})
    if error_title and error_title.text.strip().lower() == "quota exceeded":
        return True


def handle_response(request: str, response: httpx.Response) -> None:
    if response.status_code == 429:
        raise RateLimitError
    elif response.status_code != 200:
        raise Exception(
            f"Error {response.status_code} while processing request:\n{request}"
        )
    elif quota_is_exceeded(response.text):
        raise RateLimitError


def get_headers() -> Dict[str, str]:
    headers = HEADERS
    headers["User-Agent"] = USER_AGENTS[random.randint(0, len(USER_AGENTS) - 1)]
    return headers


def convert_soup_to_parts_list(soup):
    brick_elements = soup.find_all("tr", class_=BRICK_PART_CLASS)

    parts_list = {}
    for brick_element in brick_elements:
        table_elements = brick_element.find_all("td")
        part_id = table_elements[3].text.strip()
        quantity = table_elements[2].text.strip()
        if part_id not in parts_list:
            parts_list[part_id] = int(quantity)
        else:
            parts_list[part_id] += int(quantity)

    return parts_list


async def get_parts_page(set_id: str, client: httpx.AsyncClient) -> str:
    url_options = [
        f"https://www.bricklink.com/v2/catalog/catalogitem.page?S={set_id}",
        f"https://www.bricklink.com/v2/catalog/catalogitem.page?M={set_id}",
        f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={set_id}",
        f"https://www.bricklink.com/v2/catalog/catalogitem.page?B={set_id}",
    ]

    exception = None
    for url in url_options:
        try:
            response = await client.get(url, headers=get_headers())
            handle_response(url, response)
            soup = BeautifulSoup(response.text, "html.parser")

            # Find the script which contains itemID
            script = soup.find("script", text=re.compile("idItem"))
            bricklink_id = str(script).split("idItem:")[1].split(",")[0].strip()
            url = f"https://www.bricklink.com/v2/catalog/catalogitem_invtab.page?idItem={bricklink_id}"
            response = await client.get(url, headers=get_headers())
            handle_response(url, response)
            return response.text
        except RateLimitError:
            raise RateLimitError
        except Exception as e:
            exception = e
    if exception:
        raise exception


async def get_parts_list(set_id: str, client: httpx.AsyncClient) -> Dict[str, int]:
    page = await get_parts_page(set_id, client)
    soup = BeautifulSoup(page, "html.parser")
    return convert_soup_to_parts_list(soup)


def convert_soup_to_set_list(soup) -> List[str]:
    table_class = "catalog-list__body-main"
    table = soup.find("table", class_=table_class)
    rows = table.find_all("tr")[1:]
    set_list = [row.find_all("td")[1].text.split(" ")[0].strip() for row in rows]
    return set_list


async def get_sets_page(page_number: int, client: httpx.AsyncClient) -> str:
    url = f"https://www.bricklink.com/catalogList.asp?pg={page_number}&sortBy=P&sortAsc=D&catType=S"
    response = await client.get(url, headers=get_headers())
    handle_response(url, response)
    return response.text


async def get_set_list(page_number: int, client: httpx.AsyncClient) -> List[str]:
    page = await get_sets_page(page_number, client)
    soup = BeautifulSoup(page, "html.parser")
    return convert_soup_to_set_list(soup)


def get_collected_set_ids(output_filepath: str) -> List[str]:
    if os.path.exists(output_filepath):
        with open(output_filepath, "r") as f:
            already_collected_set_ids = set(f.read().splitlines())
    else:
        already_collected_set_ids = set()
    return [set_id.strip() for set_id in already_collected_set_ids if set_id != ""]


def get_all_set_ids(output_filepath: str) -> None:
    # Verify dirs
    output_dir = os.path.dirname(output_filepath)
    if not output_dir == "":
        os.makedirs(output_dir, exist_ok=True)

    collected_set_ids = get_collected_set_ids(output_filepath)

    async def get_all_set_ids_async():
        progress_bar = tqdm(total=MAX_SET_PAGE_NUMBER)
        with open(output_filepath, "a") as f:
            async with httpx.AsyncClient(timeout=30) as client:
                # Create partial of our get_set_list function
                # wrapped with the progress bar update with client
                async def get_set_list_with_progress_bar_and_save(page_number: int):
                    set_list = await get_set_list(page_number, client)
                    # Write each set id to a new line
                    for set_id in set_list:
                        if set_id not in collected_set_ids:
                            f.write(f"{set_id}\n")
                    f.flush()
                    progress_bar.update(1)

                async with aiometer.amap(
                    get_set_list_with_progress_bar_and_save,
                    range(MAX_SET_PAGE_NUMBER + 1, 1, -1),
                    max_at_once=10,
                    max_per_second=0.2,
                ) as results:
                    async for _ in results:
                        pass

        progress_bar.close()

    return asyncio.run(get_all_set_ids_async())


def open_set_ids_file(filepath: str) -> List[str]:
    with open(filepath, "r") as f:
        return [line.strip() for line in f.readlines()]


def write_parts_list(
    set_id: str, parts_list: Dict[str, int], output_filepath: str
) -> None:
    # Write to file as jsonl
    with open(output_filepath, "a") as f:
        f.write(json.dumps({"set_id": set_id, "parts_list": parts_list}))
        f.write("\n")


def get_already_collected_parts_list_set_ids(output_filepath: str) -> List[str]:
    if not os.path.exists(output_filepath):
        return []
    with open(output_filepath, "r") as f:
        return [json.loads(line)["set_id"] for line in f.readlines()]


def get_inventory_set_ids(output_filepath: str) -> List[str]:
    if not os.path.exists(output_filepath):
        return []
    with open(output_filepath, "r") as f:
        set_ids = set()
        for line in f.readlines():
            part_ids = json.loads(line)["parts_list"].keys()
            set_ids.update(
                [
                    part_id.replace("(inv)", "")
                    for part_id in part_ids
                    if "(inv)" in part_id
                ]
            )
        return list(set_ids)


def get_all_parts_lists(output_filepath: str, set_list_filepath: str) -> None:
    # Verify dirs
    output_dir = os.path.dirname(output_filepath)
    if not output_dir == "":
        os.makedirs(output_dir, exist_ok=True)

    set_list = set(open_set_ids_file(set_list_filepath))
    set_list.update(get_inventory_set_ids(output_filepath))
    already_collected_set_ids = get_already_collected_parts_list_set_ids(
        output_filepath
    )
    set_list = list(set(set_list) - set(already_collected_set_ids))

    async def get_all_parts_lists_async():
        progress_bar = tqdm(total=len(set_list))
        async with httpx.AsyncClient(timeout=30) as client:
            # Create partial of our get_parts_list function
            # wrapped with the progress bar update with client
            async def get_parts_list_with_progress_bar_and_save(set_id: str):
                try:
                    parts_list = await get_parts_list(set_id, client)
                except RateLimitError:
                    raise RateLimitError
                except Exception as e:
                    print(f"Error getting parts list for set_id: {set_id}")
                    print(e)
                    return
                write_parts_list(set_id, parts_list, output_filepath)
                progress_bar.update(1)

            async with aiometer.amap(
                get_parts_list_with_progress_bar_and_save,
                set_list,
                max_at_once=10,
                max_per_second=0.2,
            ) as results:
                async for _ in results:
                    pass

        progress_bar.close()

    return asyncio.run(get_all_parts_lists_async())


def get_unique_lego_parts_from_set_part_lists(parts_list_filepath: str) -> List[str]:
    if not os.path.exists(parts_list_filepath):
        return []
    with open(parts_list_filepath, "r") as f:
        parts_lists = [json.loads(line)["parts_list"] for line in f.readlines()]

    global_parts = set()
    for parts_list in parts_lists:
        global_parts.update(parts_list.keys())

    return list(global_parts)


async def get_lego_part_weight_in_grams(
    part_id: str, client: httpx.AsyncClient
) -> float:
    part_id = part_id.replace("(inv)", "")
    url_options = [
        f"https://www.bricklink.com/v2/catalog/catalogitem.page?P={part_id}",
        f"https://www.bricklink.com/v2/catalog/catalogitem.page?M={part_id}",
        f"https://www.bricklink.com/v2/catalog/catalogitem.page?B={part_id}",
        f"https://www.bricklink.com/v2/catalog/catalogitem.page?G={part_id}",
        f"https://www.bricklink.com/v2/catalog/catalogitem.page?S={part_id}",
    ]
    exception = None
    for url in url_options:
        try:
            response = await client.get(url, headers=get_headers())
            handle_response(url, response)
            soup = BeautifulSoup(response.text, "html.parser")
            span = soup.find("span", {"id": "item-weight-info"})
            weight_text = span.text.strip().replace("g", "")
            if weight_text == "?":
                return None
            return float(weight_text)
        except Exception as e:
            exception = e
    raise exception


def get_collected_lego_weights(output_filepath: str) -> Dict[str, float]:
    if not os.path.exists(output_filepath):
        return {}
    with open(output_filepath, "r") as f:
        weights = {}
        for line in f.readlines():
            part_id, weight = line.split(",")
            part_id, weight = part_id.strip(), weight.strip()
            if weight == "None":
                weight = None
            else:
                try:
                    weight = float(weight)
                except ValueError:
                    print(f"Error converting weight to float: {weight}")
                    weight = None
            weights[part_id] = weight
        return weights


def get_all_lego_weights(parts_list_filepath: str, output_filepath: str) -> None:
    output_dir = os.path.dirname(output_filepath)
    if not output_dir == "":
        os.makedirs(output_dir, exist_ok=True)

    parts_list = get_unique_lego_parts_from_set_part_lists(parts_list_filepath)
    collected_lego_weights = get_collected_lego_weights(output_filepath)
    parts_list = [
        part_id for part_id in parts_list if part_id not in collected_lego_weights
    ]
    parts_list.reverse()

    async def get_all_lego_weights_async():
        progress_bar = tqdm(total=len(parts_list), smoothing=0.03)
        with open(output_filepath, "a") as f:
            async with httpx.AsyncClient(timeout=30) as client:

                async def get_lego_weight_with_progress_bar_and_save(part_id: str):
                    try:
                        weight = await get_lego_part_weight_in_grams(part_id, client)
                    except RateLimitError:
                        raise RateLimitError
                    except Exception as e:
                        print(f"Error getting weight for part_id: {part_id}")
                        print(e)
                        return
                    f.write(f"{part_id},{weight}\n")
                    # Flush to file
                    f.flush()
                    progress_bar.update(1)

                async with aiometer.amap(
                    get_lego_weight_with_progress_bar_and_save,
                    parts_list,
                    max_at_once=10,
                    max_per_second=0.2,
                ) as results:
                    async for _ in results:
                        pass

        progress_bar.close()

    return asyncio.run(get_all_lego_weights_async())


if __name__ == "__main__":
    # get_all_set_ids("scraping_results/set_ids.txt")
    # get_all_parts_lists(
    #     "scraping_results/parts_lists.jsonl", "scraping_results/set_ids.txt"
    # )
    get_all_lego_weights(
        "scraping_results/parts_lists.jsonl", "scraping_results/lego_weights.csv"
    )
    # print(get_inventory_set_ids("scraping_results/parts_lists.jsonl"))
