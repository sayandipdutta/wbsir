from tqdm.asyncio import tqdm as atqdm
# from tqdm import tqdm
import argparse
import asyncio
import logging
import sqlite3
import pandas as pd
from wbsir.download import (
    get_districts_table,
    get_assembly_constituencies_table,
    get_polling_stations_table,
)

BASE_URL = "https://ceowestbengal.wb.gov.in"


async def main():
    logging.info("Fetching districts...")
    try:
        districts_df = await get_districts_table(BASE_URL)
    except Exception as e:
        logging.error(f"Error fetching districts: {e}")
        return

    logging.info(f"Found {len(districts_df)} districts.")

    # init_db()

    districts_df = (
        districts_df
        .rename(columns={"district": "name"})
        .assign(serial=lambda df: range(1, len(df) + 1))
        .drop(columns=["path"])
    )
    assert set(districts_df.columns) == {"serial", "name"}
    with sqlite3.connect("wbsir.db") as conn:
        _ = districts_df.to_sql("districts", conn, if_exists="replace", index=False)

    logging.info("Districts saved to database.")

    logging.info("Fetching assemblies...")
    try:
        futures = [
            get_assembly_constituencies_table(BASE_URL, district_id)
            for district_id in districts_df.serial
        ]
        # tables = [await future for future in tqdm(futures)]
        tables = await atqdm.gather(*futures)
        all_assemblies = pd.concat(
            [
                table.assign(district_id=district_id)
                for table, district_id in zip(tables, districts_df.serial)
            ],
            ignore_index=True,
        ).rename(columns={"AC_no.": "serial", "AC_name": "name"})[
            ["serial", "name", "district_id"]
        ]
        logging.info(f"Found {len(all_assemblies)} assemblies.")
    except Exception as e:
        logging.error(f"Error fetching assemblies: {e}")
        return
    with sqlite3.connect("wbsir.db") as conn:
        all_assemblies.to_sql("assemblies", conn, if_exists="replace", index=False)
    logging.info("Assemblies saved to database.")

    logging.info("Fetching polling stations...")
    try:
        assembly_ids = all_assemblies.serial.astype(int)
        futures = [get_polling_stations_table(BASE_URL, assembly_id) for assembly_id in assembly_ids]
        # tables = [await fut for fut in tqdm(futures)]
        tables = await atqdm.gather(*futures)
        print(tables[0].columns)
        all_polling_stations = pd.concat(
            [
                result.assign(assembly_id=assembly_id)
                for result, assembly_id in zip(tables, assembly_ids)
            ],
            ignore_index=True,
        ).rename(columns={"Ps No.": "serial", "Polling Station Name": "name", "path": "location"})[
            ["serial", "name", "location", "assembly_id"]
        ]
        logging.info(f"Found {len(all_polling_stations)} polling stations.")
    except Exception as e:
        logging.error(f"Error fetching polling stations: {e}")
        raise
        return
    with sqlite3.connect("wbsir.db") as conn:
        _ = all_polling_stations.to_sql(
            "polling_stations", conn, if_exists="replace", index=False
        )
    logging.info("Polling stations saved to database.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-l",
        "--loglevel",
        default=logging.WARNING,
        choices=logging.getLevelNamesMapping().keys(),
        help="Set log level",
    )

    args = parser.parse_args()
    logging.basicConfig(level=args.loglevel)
    logger = logging.getLogger(__name__)
    asyncio.run(main())
