import asyncio
import base64
import logging
from pathlib import Path
import ssl
from io import StringIO
from itertools import chain
from typing import Literal

import pandas as pd
from aiohttp import ClientSession, TCPConnector

type LinkLocation = Literal["body", "all", "footer"]

context = ssl.create_default_context()
context.options |= ssl.OP_LEGACY_SERVER_CONNECT


def base64_str(s: str) -> str:
    return base64.b64encode(s.encode()).decode()


async def get_url(url: str) -> str:
    connector = TCPConnector(ssl=context)
    async with (
        ClientSession(connector=connector) as session,
        session.get(url) as response,
    ):
        return await response.text()


def get_html_table(
    html: str | StringIO, extract_links: LinkLocation | None = "body"
) -> pd.DataFrame:
    assert extract_links in (None, "body", "all", "footer")
    if isinstance(html, str):
        html = StringIO(html)
    try:
        return pd.read_html(  # pyright: ignore[reportUnknownMemberType]
            html,
            flavor="lxml",
            attrs={"id": "demoGrid"},
            extract_links=extract_links,
        )[0]
    except IndexError:
        logging.warning("Could not find table")
        raise


def resolve_multi_col(df: pd.DataFrame, resolved_cols: list[str]) -> pd.DataFrame:
    return df.apply(
        lambda row: pd.Series(list(filter(None, chain(*row))), index=resolved_cols),  # pyright: ignore[reportUnknownArgumentType, reportUnknownLambdaType]
        axis="columns",
    )  # ty:ignore[invalid-return-type]


async def get_districts_table(url: str):
    url = f"{url}/Roll_dist"
    html = StringIO(await get_url(url))
    districts_table = get_html_table(html)
    return resolve_multi_col(districts_table, ["district", "path"])


async def get_assembly_constituencies_table(url: str, target_district_index: int):
    assert target_district_index > 0
    url = f"{url}/Roll_ac/{target_district_index}"
    html = await get_url(url)
    assembly_constituencies_table = get_html_table(html)
    return resolve_multi_col(
        assembly_constituencies_table, ["AC_no.", "AC_name", "path"]
    )


async def get_polling_stations_table(
    url: str, target_assembly_constituency_index: int
) -> pd.DataFrame:
    assert target_assembly_constituency_index > 0
    url = f"{url}/Roll_ps/{target_assembly_constituency_index}"
    html = await get_url(url)
    polling_stations_table = get_html_table(html, extract_links=None)
    polling_stations_table["path"] = (
        polling_stations_table["Ps No."]
        .astype(str)
        .str.zfill(3)
        .radd(f"AC{target_assembly_constituency_index:03d}PART")
        .add(".pdf")
    )
    return polling_stations_table


async def download_file(url: str, save_path: Path):
    assert save_path.parent.exists(), f"Directory {save_path.parent} does not exist"
    async with (
        ClientSession(connector=TCPConnector(ssl=context)) as session,
        session.get(url) as response,
    ):
        _ = await asyncio.to_thread(save_path.write_bytes, await response.read())


async def download_all_polling_station_pdfs(
    base_url: str,
    ac_id: int,
    save_dir: Path,
):
    ps_table = await get_polling_stations_table(base_url, ac_id)
    download_url_prefix = f"{base_url}/RollPDF/GetDraft?acId={ac_id}&key="
    ps_table["url"] = ps_table["path"].apply(base64_str).radd(download_url_prefix)  # pyright: ignore[reportUnknownMemberType]
    async with asyncio.TaskGroup() as tg:
        _tasks = list(
            map(
                tg.create_task,
                map(download_file, ps_table.url, ps_table.path.rdiv(save_dir)),
            )
        )


def main():
    base_url = "https://ceowestbengal.wb.gov.in"
    (path := Path("./data")).mkdir(parents=True, exist_ok=True)
    _ = asyncio.run(download_all_polling_station_pdfs(base_url, 1, path))


if __name__ == "__main__":
    main()
