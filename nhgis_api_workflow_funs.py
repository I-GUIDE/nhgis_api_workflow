from ipumspy import IpumsApiClient, BaseExtract, readers
from zipfile import ZipFile
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, Union, Tuple

import pandas as pd
import geopandas as gpd
import re
import requests
import os

class IpumsApiException(Exception):
    pass

class IpumsExtractFailure(IpumsApiException):
    """Represents the case when an extract fails for unknown reasons"""

class IpumsNotFound(IpumsApiException):
    """Represents the case that there is no extract with the provided id"""

class IpumsExtractNotReady(IpumsApiException):
    """Represents the case that your extract is not yet ready"""

def _extract_and_collection(extract: Union[BaseExtract, int], collection: Optional[str]) -> Tuple[int, str]:
    if isinstance(extract, BaseExtract):
        extract_id = extract.extract_id
        collection = extract.collection
    else:
        extract_id = extract
        if not collection:
            raise ValueError(
                "If ``extract`` is not a BaseExtract, ``collection`` must be non-null"
            )
    return extract_id, collection

class NhgisExtract(BaseExtract, collection="nhgis"):
    def __init__(
        self,
        datasets: Optional[Dict[str, Dict[str, list]]] = None,
        time_series_tables: Optional[Dict[str, Dict[str, list]]] = None,
        shapefiles: Optional[List[str]] = [],
        geographic_extents: Optional[List[str]] = None,
        breakdown_and_data_type_layout: Optional[str] = "single_file",
        tst_layout: Optional[str] = "time_by_column_layout",
        description: Optional[str] = "My IPUMS NHGIS Extract",
        data_format: Optional[str] = "csv_no_header",
        **kwargs,
    ):
        """
        Defining an IPUMS NHGIS extract.
        """

        super().__init__()
        self.datasets = datasets
        self.time_series_tables = time_series_tables
        self.shapefiles = shapefiles
        self.geographic_extents = geographic_extents
        self.breakdown_and_data_type_layout = breakdown_and_data_type_layout
        self.tst_layout = tst_layout
        self.description = description
        self.data_format = data_format
        self.collection = self.collection
        """Name of an IPUMS data collection"""

        # check kwargs for conflicts with defaults
        self._kwarg_warning(kwargs)

    def build(self) -> Dict[str, Any]:
        """
        Convert the object into a dictionary to be passed to the IPUMS API
        as a JSON string
        """

        build = {
            "description": self.description,
            "collection": self.collection,
            "shapefiles": self.shapefiles
        }

        if self.datasets is not None:
            build["datasets"] = self.datasets
            build["dataFormat"] = self.data_format
            build["breakdownAndDataTypeLayout"] = self.breakdown_and_data_type_layout

            if self.geographic_extents is not None:
                build["geographicExtents"] = self.geographic_extents

        if self.time_series_tables is not None:
            build["timeSeriesTables"] = self.time_series_tables
            build["timeSeriesTableLayout"] = self.tst_layout

        return build

class NhgisApiClient(IpumsApiClient):
    def __init__(
        self, 
        api_key,
        base_url: str = "https://api.ipums.org/extracts",
        api_version: str = 2,
        num_retries: int = 3,
    ):
        super().__init__(api_key, base_url, api_version, num_retries)
        self.metadata_url = "https://api.ipums.org/metadata/nhgis"

    def download_nhgis_extract(
        self,
        extract: Union[BaseExtract, int],
        collection: Optional[str] = "nhgis",
        download_dir: Optional[Union[str, Path]] = None,
    ):
        """
        Download the extract with id ``extract_number`` to ``download_dir``
        (default location is current directory)

        Args:
            extract: The extract to download. This extract must have been submitted.
                Alternatively, can be an extract id. If an extract id is provided, you
                must supply the collection name
            collection: The name of the collection to pull the extract from. If None,
                then ``extract`` must be a ``BaseExtract``
            download_dir: The location to download the data to.
                MUST be a directory that currently exists
        """
        extract_id, collection = _extract_and_collection(extract, collection)

        # if download_dir specified check if it exists
        download_dir = Path(download_dir or Path.cwd())
        if not download_dir.exists():
            raise FileNotFoundError(f"{download_dir} does not exist")

        # check to see if extract complete
        extract_status = self.extract_status(extract_id, collection=collection)
        if extract_status == "not found":
            raise IpumsNotFound(
                f"There is no IPUMS extract with extract number "
                f"{extract_id} in collection {collection}. Be sure to submit your "
                f"extract before trying to download it!"
            )
        if extract_status == "failed":
            raise IpumsExtractFailure(
                f"Your IPUMS {collection} extract number {extract_id} "
                f"failed to complete. Please resubmit your extract. "
                f"If the issue lingers, please reach out to ipums@umn.edu for assistance."
            )
        if extract_status != "completed":
            raise IpumsExtractNotReady(
                f"Your IPUMS {collection} extract number {extract_id} "
                f"is not finished yet!"
            )

        response = self.get(
            f"{self.base_url}/{extract_id}",
            params={"collection": collection, "version": self.api_version},
        )

        download_links = response.json()["downloadLinks"]
        
        try:
            # if the extract has been expired, the download_links element will be
            # an empty dict
            data_url = download_links["tableData"]["url"]
            gis_url = download_links["gisData"]["url"]
            download_urls = [data_url, gis_url]

        except KeyError:
            if isinstance(extract, BaseExtract):
                raise IpumsExtractNotReady(
                    f"IPUMS {collection} extract {extract_id} has expired and its files have been deleted.\n"
                    f"Use `submit_extract() to resubmit this extract object as a new extract request."
                )
            else:
                raise IpumsExtractNotReady(
                    f"IPUMS {collection} extract {extract_id} has expired and its files have been deleted.\n"
                    f"Use `get_extract_by_id()` and `submit_extract()` to resubmit this definition as a new extract request."
                )
            
        for url in download_urls:
            file_name = url.split("/")[-1]
            download_path = download_dir / file_name
            with self.get(url, stream=True) as response:
                response.raise_for_status()
                with open(download_path, "wb") as outfile:
                    for chunk in response.iter_content(chunk_size=8192):
                        outfile.write(chunk)

    def nhgis_metadata(
        self,
        type: Optional[str] = None,
        dataset: Optional[str] = None,
        data_table: Optional[str] = None,
        time_series_table: Optional[str] = None,
        page_size: Optional[int] = 2500
    ):
        if type is not None:
            if type not in ["datasets", "data_tables", "time_series_tables", "shapefiles"]:
                raise IpumsNotFound(
                    f"\"{type}\" is not a valid metadata endpoint."
                )
            
            metadata = self.get(
                f"{self.metadata_url}/{type}",
                params = {
                    "version": self.api_version,
                    "pageSize": page_size,
                }
            ).json()

            data = metadata["data"]
            next_page = metadata["links"]["nextPage"]

            while next_page is not None:
                next_metadata = self.get(
                    next_page,
                    params = {
                        "version": self.api_version,
                        "pageSize": page_size,
                    }
                ).json()

                next_page = next_metadata["links"]["nextPage"]
                data += next_metadata["data"]

            return data
        
        elif dataset is not None:
            if data_table is not None:
                url = f"{self.metadata_url}/datasets/{dataset}/data_tables/{data_table}"
            else:
                url = f"{self.metadata_url}/datasets/{dataset}"

        elif time_series_table is not None:
            url = f"{self.metadata_url}/time_series_tables/{time_series_table}"

        metadata = self.get(
            url,
            params = {
                "version": self.api_version,
                "pageSize": page_size,
            }
        ).json()

        return metadata
    
def nhgis_list_files(filename: str, pattern: str = None):
    with ZipFile(filename, mode="r") as z:
        files = [fname for fname in z.namelist()]

        if pattern is not None:
            files_format = "\n".join(files)
            files = [f for f in files if re.search(pattern, f)]

            if not files:
                raise IpumsNotFound(
                    f"No files matching pattern \"{pattern}\" were found. "
                    f"Available files:\n{files_format}"
                )
    
    return files

def read_nhgis(filename, file_select: Optional[str] = None):
    with ZipFile(filename, mode="r") as z:
        files = [fname for fname in z.namelist()]
        files_format = "\n".join(files)

        if file_select is not None:
            selection = [i for i, item in enumerate(z.namelist()) if re.search(file_select, item)]
        else:
            selection = [i for i, item in enumerate(z.namelist())]

        if len(selection) > 1:
            raise IpumsNotFound(
                f"Multiple files selected. "
                f"Available files:\n{files_format}"
            )
        elif len(selection) == 0:
            raise IpumsNotFound(
                f"No files matching pattern \"{file_select}\" were found. "
                f"Available files:\n{files_format}"
            )
        
        file = z.infolist()[selection[0]]

        with z.open(file) as infile:
            data = pd.read_csv(infile)
    
    return data

def read_nhgis_shp(filename, file_select: Optional[str] = None):
    with ZipFile(filename, mode="r") as z:
        files = [fname for fname in z.namelist()]
        files_format = "\n".join(files)

        if file_select is not None:
            selection = [i for i, item in enumerate(z.namelist()) if re.search(file_select, item)]
        else:
            selection = [i for i, item in enumerate(z.namelist())]

        if len(selection) > 1:
            raise IpumsNotFound(
                f"Multiple files selected. "
                f"Available files:\n{files_format}"
            )
        elif len(selection) == 0:
            raise IpumsNotFound(
                f"No files matching pattern \"{file_select}\" were found. "
                f"Available files:\n{files_format}"
            )
        
        file = z.infolist()[selection[0]]

        with z.open(file) as infile:
            data = gpd.read_file(infile)
    
    return data