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
        districts_df.rename(columns={"district": "name"})
        .assign(serial=lambda df: range(1, len(df) + 1))
        .drop(columns=["path"])
    )
    assert set(districts_df.columns) == {"serial", "name"}
    with sqlite3.connect("wbsir.db") as conn:
        _ = districts_df.to_sql("districts", conn, if_exists="replace", index=False)

    logging.info("Districts saved to database.")

    logging.info("Fetching assemblies...")
    try:
        all_assemblies = pd.concat(
            [
                (await get_assembly_constituencies_table(BASE_URL, district_id)).assign(
                    district_id=district_id
                )
                for district_id in districts_df["serial"]
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
        all_polling_stations = pd.concat(
            [
                (await get_polling_stations_table(BASE_URL, assembly_id)).assign(
                    assembly_id=assembly_id
                )
                for assembly_id in all_assemblies["serial"]
            ],
            ignore_index=True,
        ).rename(columns={"Ps No.": "serial", "Ps Name": "name"})[
            ["serial", "name", "assembly_id"]
        ]
        logging.info(f"Found {len(all_polling_stations)} polling stations.")
    except Exception as e:
        logging.error(f"Error fetching polling stations: {e}")
        return
    with sqlite3.connect("wbsir.db") as conn:
        _ = all_polling_stations.to_sql(
            "polling_stations", conn, if_exists="replace", index=False
        )
    logging.info("Polling stations saved to database.")


if __name__ == "__main__":
    asyncio.run(main())
