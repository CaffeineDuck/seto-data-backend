import typing as ty
from pathlib import Path

import httpx
from bs4 import BeautifulSoup
from dagster import Output, graph_asset, op

__all__ = ("download_customs_report",)

REPORT_SCRAPE_URL = "https://www.customs.gov.np/page/fts-fy-208081"
REPORT_LINKS_SELECTOR = "#container > div > div > div > div.style1.col-xs-12.col-sm-12.col-md-9.col-lg-9 > ul"

NEPALI_MONTHS = (
    "BAISAKH",
    "JESTHA",
    "ASHAD",
    "SHRAVAN",
    "BHADRA",
    "ASHWIN",
    "KARTIK",
    "MANGSIR",
    "POUSH",
    "MAGH",
    "FALGUN",
    "CHAITRA",
)


class TagNotFoundError(Exception):
    def __init__(self, tag_name: str) -> None:
        self.tag_name = tag_name

    def __str__(self) -> str:
        return 'Could not find tag with name "{}"'.format(self.tag_name)


class ReportLinkNotFoundError(Exception):
    def __str__(self) -> str:
        return "Report link not found"


class ReportLink(ty.NamedTuple):
    url: str
    title: str


@op
def get_soup() -> Output[BeautifulSoup]:
    with httpx.Client(verify=False) as client:
        response = client.get(REPORT_SCRAPE_URL)
        response.raise_for_status()
    return Output(BeautifulSoup(response.text, "html.parser"))


@op
def fetch_report_urls(soup: BeautifulSoup) -> Output[list[ReportLink]]:
    links_ul = soup.select_one(REPORT_LINKS_SELECTOR)
    if not links_ul:
        raise TagNotFoundError("ul")

    report_links = []
    for li_tag in links_ul.select("li"):
        link = li_tag.select_one("a")
        if not link:
            raise TagNotFoundError("a")
        report_links.append(
            ReportLink(
                url=link.attrs["href"],
                title=link.text,
            )
        )
    return Output(report_links)


@op
def get_latest_customs_report_url(report_links: list[ReportLink]) -> Output[str]:
    filtered_report_links: list[tuple[str, str]] = []

    for title, link in report_links:
        if "FTS" not in title:
            continue
        if not any(month.lower() in title for month in NEPALI_MONTHS):
            continue

        month = title.split()[-1]
        filtered_report_links.append((link, month))

    if not filtered_report_links:
        raise ReportLinkNotFoundError()

    # TODO: Filter the latest's month's data and return it.
    return Output(filtered_report_links[1][1])


@op
def download_report_from_url(url: str) -> Output[Path]:
    print(f"Downloading from {url=}")

    with httpx.Client(verify=False) as client:
        response = client.get(url)
        response.raise_for_status()

    file_path = Path("/tmp/report.xlsx")
    with open(file_path, "wb") as file:
        file.write(response.content)

    return Output(file_path)


@graph_asset
def download_customs_report() -> Path:
    soup = get_soup()
    report_urls = fetch_report_urls(soup)
    latest_report_url = get_latest_customs_report_url(report_urls)
    downloaded_path = download_report_from_url(latest_report_url)

    return downloaded_path
