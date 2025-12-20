import asyncio
import base64
import logging
import sqlite3
import ssl
from asyncio.exceptions import CancelledError
from io import StringIO
from itertools import chain
from pathlib import Path
from typing import Literal

import pandas as pd
from aiohttp import (
    ClientConnectionError,
    ClientSession,
    ConnectionTimeoutError,
    TCPConnector,
)
from tqdm.asyncio import tqdm as atqdm

from wbsir.config import BASE_URL, DATA_DIR, DATABASE_PATH

type LinkLocation = Literal["body", "all", "footer"]

context = ssl.create_default_context()
context.options |= ssl.OP_LEGACY_SERVER_CONNECT


def base64_str(s: str) -> str:
    """
    Encode a string in base64, and return encoded string

    Parameters
    ----------
    s: str
        string to encode

    Returns
    -------
    str
        return encoded string
    """
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


async def get_assembly_constituencies_table(
    url: str, target_district_index: int
) -> pd.DataFrame:
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


async def download_file(
    url: str,
    save_path: Path,
    *,
    max_retries: int = 5,
    overwrite: bool = False,
):
    assert save_path.parent.exists(), f"Directory {save_path.parent} does not exist"
    assert not save_path.is_dir(), f"{save_path} is a directory"
    if not overwrite and save_path.is_file():
        return
    try:
        async with (
            ClientSession(connector=TCPConnector(ssl=context)) as session,
            session.get(url) as response,
        ):
            # _ = await asyncio.to_thread(save_path.write_bytes, await response.read())
            _ = save_path.write_bytes(await response.read())
    except* ConnectionTimeoutError, CancelledError, ClientConnectionError:
        # TODO(sayandipdutta): Log
        if max_retries == 0:
            raise
        await download_file(
            url,
            save_path,
            max_retries=max_retries - 1,
            overwrite=overwrite,
        )


async def main():
    with sqlite3.connect(DATABASE_PATH) as conn:
        df = pd.read_sql_query(
            "SELECT location, assembly_id FROM polling_stations", conn
        )
    if df.empty:
        return
    urls: pd.Series[str] = (
        f"{BASE_URL}/RollPDF/GetDraft?acId="
        + df.assembly_id.astype(str)
        + "&key="
        + df.location.apply(base64_str)
    )
    save_paths: pd.Series[Path] = df.location.rdiv(DATA_DIR)
    DATA_DIR.mkdir(exist_ok=True)
    futures = list(map(download_file, urls, save_paths))
    async for fut in atqdm(asyncio.as_completed(futures), total=len(futures)):
        await fut


if __name__ == "__main__":
    asyncio.run(main())
