#!/usr/bin/env python3.7
# areq.py

"""Asynchronously get links embedded in multiple pages'"""

import asyncio
import logging
import re
import sys
import urllib.error
import urllib.parse
from typing import IO

from lib import aiofiles
import aiohttp
from aiohttp import ClientSession, http_exceptions

logging.basicConfig(
    format='%(asctime)s %(levelname)s:%(name)s: %(message)s',
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr
)
logger = logging.getLogger('areq')
logging.getLogger('chardet.charsetprober').disabled = True

HREF_RE = re.compile(r'href="(.*?)"')


async def fetch_html(url: str, session: ClientSession, **kwargs) -> str:
    """
    GET request wrapper to fetch page HTML.
    :param url:
    :param session:
    :param kwargs: session.request()
    :return: str
    """
    resp = await session.request(method='GET', url=url, **kwargs)
    resp.raise_for_status()
    logger.info('Got response [%s] for URL: %s', resp.status, url)
    html = await resp.text()
    return html


async def parse(url: str, session: ClientSession, **kwargs) -> set:
    """
    Find HREFs in the html of url
    :param url:
    :param session:
    :param kwargs:
    :return: set
    """
    found = set()
    try:
        html = await fetch_html(url=url, session=session, **kwargs)
    except(
            aiohttp.ClientError,
            aiohttp.http_exceptions.HttpProcessingError
    ) as e:
        logger.error(
            'aiohttp exception for %s [%s]: %s',
            url,
            getattr(e, 'status', None),
            getattr(e, 'message', None)
        )
        return found
    except Exception as e:
        logger.exception(
            'Non-aiohttp exception occurred: %s ', getattr(e, '__dict__')
        )
        return found
    else:
        for link in HREF_RE.findall(html):
            try:
                abs_link = urllib.parse.urljoin(url, link)
            except (urllib.error.URLError, ValueError):
                logger.exception('Error parsing URL: %s', link)
                pass
            else:
                found.add(abs_link)
        logger.info('Found %d links for %s', len(found), url)
        return found


async def write_one(file: IO, url: str, **kwargs) -> None:
    """
    Write the found HREFS from url to file
    :param file:
    :param url:
    :param kwargs:
    :return: None
    """
    res = await parse(url=url, **kwargs)
    if not res:
        return None
    async with aiofiles.open(file, 'a') as f:
        for p in res:
            await f.write(f'{url}\t{p}\n')
        logger.info('Wrote results for source URL: %s', url)


async def bulk_crawl_and_write(file: IO, urls: set, **kwargs) -> None:
    """
    Crawl and write concurrently to file from multiple urls
    :param file:
    :param urls:
    :param kwargs:
    :return: None
    """
    async with ClientSession() as session:
        tasks = []
        for url in urls:
            tasks.append(
                write_one(file=file, url=url, session=session, **kwargs)
            )
            await asyncio.gather(*tasks)


if __name__ == '__main__':
    import pathlib
    import sys

    assert sys.version_info >= (3, 7), "Script requires Python 3.7+"
    here = pathlib.Path(__file__).parent

    with open(here.joinpath('urls.txt')) as infile:
        urls = set(map(str.strip, infile))

    out_path = here.joinpath('found_urls.txt')
    with open(out_path, 'w') as out_path:
        out_path.write('source_url\tparsed_url\n')

    asyncio.run(bulk_crawl_and_write(file=out_path, urls=urls))
