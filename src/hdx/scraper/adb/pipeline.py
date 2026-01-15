#!/usr/bin/python
"""Adb scraper"""

import logging
from typing import Any

import requests
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.location.country import Country
from hdx.utilities.base_downloader import DownloadError
from hdx.utilities.retriever import Retrieve
from slugify import slugify

logger = logging.getLogger(__name__)


class Pipeline:
    def __init__(self, configuration: Configuration, retriever: Retrieve, tempdir: str):
        self._configuration = configuration
        self._retriever = retriever
        self._tempdir = tempdir

    def get_countries(self) -> list:
        """Get list of countries (economies) with country code and name"""
        economies_url = f"{self._configuration['base_url']}/structure/codelist/{self._configuration['agency_id']}/CL_ECONOMY_CODES/+?format=sdmx-json"
        economies = self._retriever.download_json(economies_url)

        codelists = economies["data"]["codelists"]
        economy_codelist = codelists[0]

        countries = []
        for code in economy_codelist["codes"]:
            id = code["id"]
            name = Country.get_country_name_from_iso3(id)
            if name:
                countries.append({"id": id, "name": name})
        return sorted(countries, key=lambda c: c["name"])

    def get_dataflows(self) -> list:
        """Get list of all dataflows with dataflow id and name"""
        dataflows_url = f"{self._configuration['base_url']}/structure/dataflow/all/all/+?format=sdmx-json"
        dataflows_data = self._retriever.download_json(dataflows_url)

        dataflows = []
        for dataflow in dataflows_data["data"]["dataflows"]:
            dataflows.append(
                {
                    "id": dataflow["id"],
                    "name": dataflow["name"],
                }
            )
        return dataflows

    def get_indicators_by_dataflow(self, dataflow_id: str) -> list:
        """Get list of indicator codes for a specific dataflow"""
        indicators_url = f"https://kidb.adb.org/api/dataflow/indicators/{dataflow_id}"

        try:
            indicators = self._retriever.download_json(indicators_url)
        except DownloadError as e:
            logger.warning(f"Skipping indicators for {dataflow_id}: {e}")
            return []
        except Exception as e:
            logger.warning(f"Skipping indicators for {dataflow_id} due to error: {e}")
            return []

        return [r["code"] for r in indicators if "code" in r]

    def chunks(self, lst: list, n: int):
        """Split list into chunks"""
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    def get_sdmx_data_for_country_dataflow(
        self, dataflow_id: str, indicator_chunk: list, country_code: str
    ) -> dict | None:
        """
        Query API for a chunk of indicators for a specific country and
        dataflow. Results are cached locally with unique filename.

        Args:
            dataflow_id: ID of the dataflow
            indicator_chunk: list of indicator codes
            country_code: ISO country code

        Returns:
            sdmx-json formatted dict if successful, None if request fails
        """
        base_url = self._configuration["base_url"]
        agency_id = self._configuration.get("agency_id", "ADB")
        end_year = self._configuration.get("end_year", 2025)
        start_year = end_year - 1

        indicators_part = "+".join(indicator_chunk)
        key = f"A.{indicators_part}.{country_code}"
        url = f"{base_url}/data/{agency_id},{dataflow_id}/{key}?format=sdmx-json&startPeriod={start_year}&endPeriod={end_year}"

        # Create unique filename from indicator code and length of chunk
        first_ind = indicator_chunk[0].lower()
        count = len(indicator_chunk)
        filename = (
            f"adb-{dataflow_id.lower()}-{country_code.lower()}-"
            f"{first_ind}-plus-{count - 1}.json"
        )

        try:
            return self._retriever.download_json(url, filename)
        except requests.exceptions.JSONDecodeError:
            logger.warning(
                f"Non-JSON response for {country_code=} {dataflow_id=} indicators={len(indicator_chunk)}. "
                f"Skipping chunk. URL: {url}"
            )
            return None

    def flatten_sdmx_json(
        self,
        payload: dict,
        dataflow_name: str,
    ) -> list:
        """
        Flatten sdmx-json payload to list of observations

        Args:
            payload: sdmx-json formatted response from API
            dataflow_name: full name of dataflow

        Returns:
            List of dictionaries, one per observation with keys:
            - economy_code: ISO code for economy (aka country)
            - economy_name: full name of economy
            - dataflow: name of dataflow
            - indicator: name of indicator
            - year: year of observation
            - value: numeric value of observation
            - unit_of_measure: unit for the value
        """
        if not payload:
            return []

        dataset, structure = self._get_dataset_and_structure(payload)
        if not dataset or not structure:
            return []

        time_lookup = self._create_time_lookup(structure)
        series_dims = self._get_series_dimensions(structure)
        obs_attr = self._get_observation_attributes(structure)

        return self._create_rows(
            dataset=dataset,
            series_dims=series_dims,
            time_lookup=time_lookup,
            obs_attr=obs_attr,
            dataflow_name=dataflow_name,
        )

    def _get_dataset_and_structure(self, payload: dict) -> tuple[dict, dict]:
        """Get dataset and structure from sdmx payload"""
        data = payload.get("data") or {}
        datasets = data.get("datasets") or [None]
        structures = data.get("structures") or [None]

        dataset = datasets[0] or {}
        struct = structures[0] or {}

        return dataset, struct

    def _create_time_lookup(self, struct: dict) -> dict[str, str]:
        """Create lookup table for time period values by index"""
        dimensions = struct.get("dimensions") or {}
        observation_dims = dimensions.get("observation") or []

        if not observation_dims:
            return {}

        time_values = observation_dims[0].get("values") or []

        return {
            str(i): (time_values[i] or {}).get("value")
            for i in range(len(time_values))
            if time_values[i]
        }

    def _get_series_dimensions(self, struct: dict) -> list[dict]:
        """Get series dimensions from structure"""
        dimensions = struct.get("dimensions") or {}
        return dimensions.get("series") or []

    def _get_observation_attributes(self, struct: dict) -> list[dict]:
        """Get observation attribute definitions from structure"""
        attributes = struct.get("attributes") or {}
        return attributes.get("observation") or []

    def _get_series_value(
        self,
        series_key: str,
        keypos: int,
        series_dims: list[dict],
    ) -> dict:
        """
        Get decoded dimension value for series key at a given position

        Returns dict with 'id' and 'name' keys, or empty dict if not found
        """
        parts = series_key.split(".")
        part_index = keypos - 1  # keyPosition is 1-based

        if part_index < 0 or part_index >= len(parts):
            return {}

        # Find dimension with matching keyPosition
        dim = None
        for d in series_dims:
            if d.get("keyPosition") == keypos:
                dim = d
                break

        if not dim:
            return {}

        values = dim.get("values") or []

        try:
            value_index = int(parts[part_index])
        except (ValueError, TypeError):
            return {}

        if 0 <= value_index < len(values):
            return values[value_index] or {}

        return {}

    def _decode_attribute(self, attr_def: dict, raw_value: Any) -> Any:
        """Decode attribute value using attribute definition"""
        if raw_value is None:
            return None

        values = attr_def.get("values") or []

        if values and isinstance(raw_value, int) and 0 <= raw_value < len(values):
            decoded = values[raw_value] or {}
            return decoded.get("name") or decoded.get("id")

        return raw_value

    def _create_rows(
        self,
        *,
        dataset: dict,
        series_dims: list,
        time_lookup: dict,
        obs_attr: list,
        dataflow_name: str,
    ) -> list:
        """Build flattened rows from dataset"""
        rows = []
        series_data = dataset.get("series") or {}

        for series_key, series_obj in series_data.items():
            indicator = self._get_series_value(series_key, 2, series_dims)
            economy = self._get_series_value(series_key, 3, series_dims)

            observations = (series_obj or {}).get("observations") or {}

            for time_index, observation_data in observations.items():
                if not observation_data:
                    continue

                value = observation_data[0]
                year = time_lookup.get(time_index)

                # Decode observation attributes
                attrs = {}
                for attr_index, attr_def in enumerate(obs_attr):
                    attr_id = attr_def.get("id")
                    if not attr_id:
                        continue

                    raw_value = (
                        observation_data[attr_index + 1]
                        if (attr_index + 1) < len(observation_data)
                        else None
                    )
                    attrs[attr_id] = self._decode_attribute(attr_def, raw_value)

                rows.append(
                    {
                        "economy_code": economy.get("id"),
                        "economy_name": economy.get("name"),
                        "dataflow": dataflow_name,
                        "indicator": indicator.get("name"),
                        "year": year,
                        "value": value,
                        "unit_of_measure": attrs.get("UNIT"),
                    }
                )

        return rows

    def get_indicators_per_country(self, chunk_size: int = 15):
        """
        Iterate through all countries and dataflows (excluding SDG),
        fetching indicators in chunks to avoid API limits. For each country,
        query all applicable indicators across all dataflows and return the
        flattened data.

        Args:
            chunk_size: number of indicators to request per API call, default is 15

        Yields:
            dict: one dictionary per country with keys:
                - economy_code: ISO code for country
                - economy_name: full country name
                - data: list of observation dictionaries containing:
                    - economy_code, economy_name, dataflow, indicator, year,
                      value, unit_of_measure

        Note:
            - SDG dataflows are excluded from the query
            - Empty dataflows are skipped
            - Failed API requests return None and are skipped
        """
        countries = self.get_countries()
        dataflows = [
            df for df in self.get_dataflows() if not df.get("id", "").startswith("SDG")
        ]

        # Build indicators map once
        indicators_by_dataflow = {}
        for dataflow in dataflows:
            dataflow_id = dataflow["id"]
            indicators = self.get_indicators_by_dataflow(dataflow_id)
            if indicators:
                indicators_by_dataflow[dataflow_id] = indicators

        for country in countries[:1]:
            economy_code = country["id"]
            economy_name = country["name"]

            all_rows = []
            for dataflow in dataflows[:1]:
                dataflow_id = dataflow["id"]

                indicators = indicators_by_dataflow.get(dataflow_id, [])
                if not indicators:
                    continue

                for ind_chunk in list(self.chunks(indicators, chunk_size)):
                    payload = self.get_sdmx_data_for_country_dataflow(
                        dataflow_id=dataflow_id,
                        indicator_chunk=ind_chunk,
                        country_code=economy_code,
                    )

                    if payload is None:
                        continue

                    rows = self.flatten_sdmx_json(
                        payload=payload,
                        dataflow_name=dataflow["name"],
                    )
                    all_rows.extend(rows)

            yield {
                "economy_code": economy_code,
                "economy_name": economy_name,
                "data": all_rows,
            }

    def generate_dataset(self, economy_code: str, rows: list) -> Dataset | None:
        end_year = self._configuration.get("end_year", 2025)
        start_year = end_year - 1
        economy_name = Country.get_country_name_from_iso3(economy_code)
        dataset_title = f"{economy_name} - Key Indicators"
        dataset_name = slugify(dataset_title)

        dataset = Dataset(
            {
                "name": dataset_name,
                "title": dataset_title,
            }
        )

        dataset.add_country_location(economy_code)
        dataset.add_tags(self._configuration["tags"])
        dataset.set_time_period_year_range(start_year, end_year)

        # Add resources here
        resource_name = f"{economy_code.lower()}-key-indicators.csv"
        resource_data = {
            "name": resource_name,
            "description": f"Key indicators for {economy_name}",
        }

        dataset.generate_resource(
            folder=self._tempdir,
            filename=resource_name,
            rows=rows,
            resourcedata=resource_data,
        )

        return dataset
