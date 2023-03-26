#!/usr/bin/python
"""
UCDP:
-----

Generates HXlated API urls from the UCDP website.

"""
import logging

from hdx.data.dataset import Dataset
from hdx.data.showcase import Showcase
from hdx.location.country import Country
from hdx.utilities.dateparse import parse_date
from hdx.utilities.dictandlist import dict_of_lists_add
from slugify import slugify

logger = logging.getLogger(__name__)
hxltags = {
    "year": "#date+year",
    "side_a": "#group+name+first",
    "side_b": "#group+name+second",
    "source_article": "#meta+source",
    "source_headline": "#description",
    "where_coordinates": "#loc+name",
    "adm_1": "#adm1+name",
    "adm_2": "#adm2+name",
    "latitude": "#geo+lat",
    "longitude": "#geo+lon",
    "country": "#country+name",
    "iso3": "#country+code",
    "region": "#region+name",
    "date_start": "#date+start",
    "date_end": "#date+end",
    "best": "#affected+killed",
}


def get_countriesdata(download_url, downloader):
    countrynameisomapping = dict()
    countriesdata = dict()
    headers, iterator = downloader.get_tabular_rows(
        download_url, headers=1, dict_form=True, format="csv"
    )
    countries = list()
    for row in iterator:
        countryname = row["country"]
        countryiso = countrynameisomapping.get(countryname)
        if countryiso is None:
            countryiso, _ = Country.get_iso3_country_code_fuzzy(
                countryname, exception=ValueError
            )
            if not countryiso:
                logger.warning(f"No ISO 3 code found for {countryname}!")
                continue
            countrynameisomapping[countryname] = countryiso
            countries.append(
                {
                    "iso3": countryiso,
                    "countryname": Country.get_country_name_from_iso3(countryiso),
                    "origname": countryname,
                }
            )
        row["iso3"] = countryiso
        dict_of_lists_add(countriesdata, countryiso, row)
    index = None
    for i, header in enumerate(headers):
        if header == "country":
            index = i + 1
            break
    headers.insert(index, "iso3")
    return countries, headers, countriesdata


def generate_dataset_and_showcase(folder, country, countrydata, headers):
    """ """
    countryiso = country["iso3"]
    countryname = country["countryname"]
    title = f"{countryname} - Data on Conflict Events"
    logger.info(f"Creating dataset: {title}")
    slugified_name = slugify(f"UCDP Data for {countryname}").lower()
    dataset = Dataset(
        {
            "name": slugified_name,
            "title": title,
        }
    )
    dataset.set_maintainer("196196be-6037-4488-8b71-d786adf4c081")
    dataset.set_organization("hdx")
    dataset.set_expected_update_frequency("Every month")
    dataset.set_subnational(True)
    dataset.add_country_location(countryiso)
    tags = ["hxl", "conflict-violence"]
    dataset.add_tags(tags)

    filename = f"conflict_data_{countryiso}.csv"
    resourcedata = {
        "name": f"Conflict Data for {countryname}",
        "description": "Conflict data with HXL tags",
    }

    def process_dates(row):
        startdate = parse_date(row["date_start"])
        enddate = parse_date(row["date_end"])
        return {"startdate": startdate, "enddate": enddate}

    quickcharts = {
        "cutdown": 2,
        "cutdownhashtags": ["#date+year", "#adm1+name", "#affected+killed"],
    }
    success, _ = dataset.generate_resource_from_iterator(
        headers,
        countrydata,
        hxltags,
        folder,
        filename,
        resourcedata,
        date_function=process_dates,
        quickcharts=quickcharts,
    )
    if success is False:
        logger.warning(f"{countryname} has no data!")
        return None, None

    showcase = Showcase(
        {
            "name": f"{slugified_name}-showcase",
            "title": title,
            "notes": f"Conflict Data Dashboard for {countryname}",
            "url": f"https://ucdp.uu.se/#country/{countrydata[0]['country_id']}",
            "image_url": "https://pbs.twimg.com/profile_images/832251660718178304/y-LWa5iK_200x200.jpg",
        }
    )
    showcase.add_tags(tags)
    return dataset, showcase
