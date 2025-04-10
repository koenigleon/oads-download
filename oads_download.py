#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (C) 2025 TROPOS
# This file is licensed under the Apache License, Version 2.0.
# See the LICENSE file in the repository root for details.
#
__author__ = "Leonard König"
__email__ = "koenig@tropos.de"
__date__ = "2025-04-10"
__version__ = "3.0.0"
__description__ = """This is a Python script designed to download EarthCARE satellite
data from ESA's Online Access and Distribution System (OADS) using
the OpenSearch API of the Earth Observation Catalogue (EO-CAT).
Search queries and the general behaviour of the script can be
customised using command line arguments. To see all available
options, execute the help command: `-h`. This script is based on a
Juper notebook provided to the author by ESA."""

import sys
import os
import re
import time
import argparse
from argparse import RawTextHelpFormatter
import datetime
try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib # type: ignore
from zipfile import ZipFile, BadZipFile
import urllib.parse as urlp
import logging
from logging import Logger
from dataclasses import dataclass
from itertools import islice
from typing import Final, TypeAlias
import json

import requests
import numpy as np
import pandas as pd
from pandas._libs.tslibs.parsing import DateParseError
from bs4 import BeautifulSoup
from lxml import html

# Custom types
Orbit: TypeAlias = int
Frame: TypeAlias = str
OrbitAndFrame: TypeAlias = str
DictJSON: TypeAlias = dict

# Constants
# General script behaviour (can be edited here as required):
CHUNK_SIZE_BYTES: Final[int] = 256 * 1024 # Represents 256 KB
MAX_NUM_ORBITS_PER_REQUEST: Final[int] = 50 # Large request are split accoring to this chunk size
MAX_NUM_RESULTS_PER_REQUEST: Final[int] = 2000 # Since large requests are split into chunks, this should be more than enouth
MAX_NUM_LOGS: Final[int | None] = 10 # Set to None for no limit
MAX_AGE_LOGS: Final[pd.Timedelta | None] = None # Time period, e.g. pd.Timedelta(weeks=4)
MAX_DOWNLOAD_ATTEMPTS_PER_FILE: Final[int] = 3 # Maximum number of times a download request is repeated on error
# Level subfolder names (can be edited here as required):
SUBDIR_NAME_AUX_FILES: Final[str] = 'Meteo_Supporting_Files'
SUBDIR_NAME_ORB_FILES: Final[str] = 'Orbit_Data_Files'
SUBDIR_NAME_L0__FILES: Final[str] = 'L0'
SUBDIR_NAME_L1B_FILES: Final[str] = 'L1'
SUBDIR_NAME_L1C_FILES: Final[str] = 'L1'
SUBDIR_NAME_L2A_FILES: Final[str] = 'L2a'
SUBDIR_NAME_L2B_FILES: Final[str] = 'L2b'
# Don't change these:
FRAMES: Final[str] = 'ABCDEFGH'
NUM_FRAMES: Final[int] = 8
PROGRAM_NAME: Final[str] ='oads_download'
SETUP_INSTRUCTIONS = """!!! Note: A configuration file containing your OADS credentials is required.
!!! If you don't have one yet, simply create a file called 'config.toml'
!!! in the script's folder and enter the following content:
───config.toml───────────────────────────────────────────────────────────────────────────────────
[Local_file_system]
data_directory = '' # This is where the data is downloaded to

[OADS_credentials]
username = 'your_username'
password = \"\"\"your_password\"\"\" # Use triple quotation marks to allow for special characters
# You need to comment out or remove all collections to which you do not have access rights to
collections = [
    'EarthCAREL0L1Products',      # EarthCARE L0 and L1 Products  ! ONLY FOR COMMISSIONING TEAM USERS !
    'EarthCAREL1Validated',       # EarthCARE L1 Products
    'EarthCAREL1InstChecked',     # EarthCARE L1 Products         ! ONLY FOR CAL/VAL USER             !
    'EarthCAREL2Validated',       # EarthCARE ESA L2 Products
    'EarthCAREL2InstChecked',     # EarthCARE ESA L2 Products     ! ONLY FOR CAL/VAL USER             !
    'EarthCAREL2Products',        # EarthCARE ESA L2 Products     ! ONLY FOR COMMISSIONING TEAM USERS !
    'JAXAL2Validated',            # EarthCARE JAXA L2 Products
    'JAXAL2InstChecked',          # EarthCARE JAXA L2 Products    ! ONLY FOR CAL/VAL USER             !
    'JAXAL2Products',             # EarthCARE JAXA L2 Products    ! ONLY FOR COMMISSIONING TEAM USERS !
    'EarthCAREAuxiliary',         # EarthCARE Auxiliary Data      ! ONLY FOR CAL/VAL USER             !
    'EarthCAREXMETL1DProducts10', # EarthCARE Meteorological Data
    'EarthCAREOrbitData',         # EarthCARE Orbit Data
]
─────────────────────────────────────────────────────────────────────────────────────────────────
"""

# Custom exceptions
class InvalidInputError(Exception): pass
class BadResponseError(Exception): pass

@dataclass
class SearchRequest():
    """This class contains all data required as input for the URL template of the OpenSearch API request to EO-CAT."""
    collection_identifier_list: list[str]
    product_type: str | None = None
    product_version: str | None = None
    radius: str | None = None
    lat: str | None = None
    lon: str | None = None
    bbox: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    orbit_number: str | None = None
    frame_id: str | None = None

    def low_detail_summary(self):
        msg = f"{self.product_type}"
        if self.product_version:
            msg = f"{msg}:{self.product_version}"
        if self.start_time and self.end_time:
            if self.start_time == self.end_time:
                msg = f"{msg}, time={self.start_time}"
            else:
                msg = f"{msg}, time={self.start_time}/{self.end_time}"
        if self.radius and self.lat and self.lon:
            msg = f"{msg}, radius=({self.radius}m, {self.lat}N, {self.lon}E)"
        if self.bbox:
            msg = f"{msg}, bbox={self.bbox}"
        if self.frame_id:
            msg = f"{msg}, frame={self.frame_id}"
        if self.orbit_number:
            o_msg = f"{self.orbit_number}"
            if len(o_msg.split(',')) > 6:
                o_msg = ','.join(o_msg.split(',')[0:2]) + f",... {len(o_msg.split(',')) - 4} more orbits ...," + ','.join(o_msg.split(',')[-2:])
            msg = f"{msg}, orbits={o_msg.replace(',', ', ')}"
        return msg

def log_heading(text: str, logger: Logger, is_mayor: bool = False, line_length: int = 60) -> None:
    top_left = '#' if is_mayor else '+'
    top_right = '#' if is_mayor else '+'
    bottom_right = '#' if is_mayor else '+'
    bottom_left = '#' if is_mayor else '+'
    vertical = '#' if is_mayor else '|'
    horizontal = '=' if is_mayor else '-'
    
    if is_mayor:
        half_padding = (line_length - len(text)) / 2
        padding_left = int(np.floor(half_padding))
        padding_right = int(np.ceil(half_padding))
    else:
        padding_left = 1
        padding_right = line_length - len(text) - 1

    logger.info(top_left + horizontal * line_length + top_right)
    logger.info(vertical + ' ' * padding_left + text + ' ' * padding_right + vertical)
    logger.info(bottom_left + horizontal * line_length + bottom_right)

# --- Set up logging --------------------------------------
def remove_old_logs(max_num_logs: int | None = None, max_age_logs: pd.Timedelta | None = None) -> None:
    """Deletes old log files depending on given maximum file number and/or age"""
    logs_dirpath = os.path.abspath('logs')
    
    if os.path.exists(logs_dirpath):
        pattern = r".*oads_download_[0-9]{8}T[0-9]{6}(|_[0-9]*).log"
        if max_num_logs:
            old_logs = [os.path.abspath(os.path.join(logs_dirpath, fp)) for fp in os.listdir(logs_dirpath) if re.search(pattern, fp)]
            if len(old_logs) > max_num_logs-1:
                old_logs.sort(reverse=True)
                for log in old_logs[max_num_logs-1::]:
                    os.remove(log)
        
        if max_age_logs:
            current_time = pd.Timestamp(time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())))
            last_allowed_time = current_time - max_age_logs
            old_logs = [os.path.abspath(os.path.join(logs_dirpath, fp)) for fp in os.listdir(logs_dirpath) if re.search(pattern, fp)]
            for log in old_logs:
                log_time = pd.Timestamp(os.path.basename(log).split('.')[0].split('_')[-1])
                if log_time < last_allowed_time:
                    os.remove(log)

def console_exclusive_info(*values: object, end: str | None = "\n") -> None:
    """Wrapper for print function (forcibly flush the stream) and without logging"""
    print(*values, end=end, flush=True)

class UnlabledInfoLoggingFormatter(logging.Formatter):
    """Logging formatter that omits level name for INFO messages."""
    def format(self, record):
        if record.levelname == "INFO":
            return record.getMessage()
        return f"[{record.levelname}] {record.getMessage()}"

def create_logger(log_to_file: bool, debug: bool = False) -> Logger:
    """Creates logger with special handlers for console and optionally log files."""
    logger = logging.getLogger(PROGRAM_NAME)
    logger.setLevel(logging.DEBUG)

    # console logs
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(UnlabledInfoLoggingFormatter())
    if debug:
        console_handler.setLevel(logging.DEBUG)
    else:
        console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    # file logs (optional)
    if log_to_file:
        ensure_directory('logs')
        
        log_filename = f"logs/oads_download_{time.strftime('%Y%m%dT%H%M%S', time.localtime(time.time()))}.log"
        # Ensure that a new log is created instead of appending to an existing log 
        new_log_filename = log_filename
        i = 2
        while os.path.exists(new_log_filename):
            new_log_filename = log_filename.replace('.log', f'_{i}.log', )
            i = i + 1

        file_handler = logging.FileHandler(new_log_filename, mode="a")
        file_formatter = logging.Formatter("[%(asctime)s] [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        file_handler.setFormatter(file_formatter)
        file_handler.setLevel(logging.DEBUG)
        logger.addHandler(file_handler)
    return logger

def ensure_directory(dirpath: str) -> None:
    """Creates directory if not existing"""
    if not os.path.exists(dirpath):
        os.mkdir(dirpath)
# ---------------------------------------------------------

def get_request(url: str, logger: Logger | None = None, **kwargs) -> requests.Response:
    """Sends a GET request, validates it's response and returns it."""
    if logger: logger.debug(f"Send GET request: {url}")
    response = requests.get(url, **kwargs)
    validate_request_response(response)
    return response

def get_url_of_queryables(data: DictJSON) -> str:
    """Finds queryables url in JSON data and raises `ValueError` if not found."""
    matching_hrefs = []
    for link in data.get("links", []):
        if link.get("rel") == "http://www.opengis.net/def/rel/ogc/1.0/queryables":
            matching_hrefs.append(link.get("href"))
    if len(matching_hrefs) == 0:
        raise ValueError(f"Can not find queryables url.")
    return matching_hrefs[0]

def get_url_of_items(data: DictJSON) -> str:
    """Finds items url in JSON data and raises `ValueError` if not found."""
    matching_hrefs = []
    for link in data.get("links", []):
        if link.get("rel") == "items":
            matching_hrefs.append(link.get("href"))
    if len(matching_hrefs) == 0:
        raise ValueError(f"Can not find items url.")
    return matching_hrefs[0]

def get_url_of_collection_items(
    collection_identifier: str,
    logger: Logger | None = None,
) -> str:
    """Finds items url of given collection."""
    url_entrypoint = 'https://eocat.esa.int/collections'
    if logger: logger.debug(f"Entrypoint: {url_entrypoint}")

    response = get_request(url_entrypoint, logger=logger)

    data = json.loads(response.text)
    url_collections_queryables = get_url_of_queryables(data)
    if logger: logger.debug(f"Collections queryables: {url_collections_queryables}")

    # response = get_request(url_collections_queryables, logger=logger)
    # data = json.loads(response.text)
    # if logger: logger.debug(list(data.keys()))
    # data['properties']

    url_earthcare_collections = f"{url_entrypoint}?&title=earthcare&limit=100"
    if logger: logger.debug(f"Search for EarthCARE collections: {url_earthcare_collections}")
    response = get_request(url_earthcare_collections, logger=logger)
    # if logger: logger.debug(response)

    data_collections = json.loads(response.text)
    available_earthcare_collections = [d['id'] for d in data_collections['collections']]
    if logger: logger.debug(f"Available EarthCARE collections: {available_earthcare_collections}")

    data_collection = [d for d in data_collections['collections'] if d['id'] == collection_identifier][0]
    # url_collection_queryables = get_url_of_queryables(data_collection)
    # if logger: logger.debug(url_collection_queryables)

    # response = get_request(url_collection_queryables, logger=logger)
    # data_collection_queryables = json.loads(response.text)
    # # if logger: logger.debug(list(data_collection_queryables.keys()))
    # if logger: logger.debug(list(data_collection_queryables['properties'].keys()))

    url_collection_items = get_url_of_items(data_collection)
    # if logger: logger.debug(url_collection_items)

    return url_collection_items

def validate_request_response(
    response: requests.models.Response,
    logger: Logger | None = None,
) -> None:
    """Raises HTTPError if one occurred and logs it."""
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        if logger: logger.exception(e)
        raise

def validate_combination_of_given_orbit_and_frame_range_inputs(
    start_orbit_and_frame: Frame | None,
    end_orbit_and_frame: Frame | None,
    start_orbit_number: Orbit | None,
    end_orbit_number: Orbit | None,
    orbit_numbers: list[Orbit] | None,
    frame_ids: list[Frame] | None,
    logger: Logger | None = None
) -> None:
    """Raises an InvalidInputError, if combined orbit and frame options (-soaf, -eoaf) are used in combination with any other orbit (-o, -so, -eo) or frames (-f) option (exception: -oaf)."""
    try:
        if ((start_orbit_and_frame is not None or end_orbit_and_frame is not None) and
            (start_orbit_number is not None or end_orbit_number is not None or orbit_numbers is not None or frame_ids is not None)):
            exception_msg = f"Options to select a range of obit and frame names (-soaf, -eoaf) can not be used in combination with the options to select only a range of orbits (-o, -so, -eo) or single frames (-f)."
            raise InvalidInputError(exception_msg)
    except InvalidInputError as e:
        if logger: logger.exception(e)
        raise

def get_validated_orbit_number(orbit_number: Orbit, logger: Logger | None = None) -> int:
    """Raises InvalidInputError if orbit number is negative or too large"""
    try:
        if orbit_number < 0 or orbit_number > 99999:
            exception_msg = f"{orbit_number} is not a valid orbit number. Valid orbit numbers are positive integers up to 5 digits."
            raise InvalidInputError(exception_msg)
    except InvalidInputError as e:
        if logger: logger.exception(e)
        raise
    return orbit_number

def get_validated_frame_id(frame_id: str, logger: Logger | None = None) -> str:
    """Formats frame ID and raises InvalidInputError if it is invalid"""
    try:
        frame_id = frame_id.upper()
        if len(frame_id) != 1:
            exception_msg = f"Got an empty string as frame ID. Valid frames are single letters from A to H."
            raise InvalidInputError(exception_msg)
        if frame_id not in 'ABCDEFGH':
            exception_msg = f"{frame_id} is not a valid frame ID. Valid frames are single letters from A to H."
            raise InvalidInputError(exception_msg)
    except InvalidInputError as e:
        if logger: logger.exception(e)
        raise
    return frame_id

def get_validated_orbit_and_frame(orbit_and_frame: OrbitAndFrame, logger: Logger | None = None) -> tuple[Orbit, Frame]:
    """Extracts validated orbit number and frame ID from string and raises InvalidInputError if string does not describe a orbit and frame"""
    try:
        orbit_number = get_validated_orbit_number(int(orbit_and_frame[0:-1]))
        frame_id = get_validated_frame_id(orbit_and_frame[-1])
    except Exception as e:
        exception_msg = f"{orbit_and_frame} is not a valid orbit and frame name. Valid names contain the orbit number followed by the frame id letter (e.g. 3000B or 03000B)."
        if logger: logger.exception(exception_msg)
        raise
    return orbit_number, frame_id

def get_validated_selected_index(selected_index: int | None, logger: Logger | None = None) -> int | None:
    """Converts 1-indexed selected_index to 0-indexed and raises InvalidInputError if it is 0"""
    try:
        if selected_index is None:
            return None
        else:
            if selected_index >= 1:
                selected_index = selected_index - 1
            elif selected_index == 0:
                raise InvalidInputError("The indices in the found files list start at 1.")
            return selected_index
    except InvalidInputError as e:
        if logger: logger.exception(e)
        raise

def get_validated_orbit_number_range(
    start_orbit_number: Orbit | None,
    end_orbit_number: Orbit | None,
    logger: Logger | None = None
) -> list[Orbit] | None:
    """Returns all orbits within range and raises InvalidInputError if given arguments are invalid"""
    try:
        if (start_orbit_number is None and end_orbit_number is not None):
            raise InvalidInputError(f"End orbit was given ({end_orbit_number}) but start is missing.")
        elif (start_orbit_number is not None and end_orbit_number is None):
            raise InvalidInputError(f"Start orbit was given ({start_orbit_number}) but end is missing.")
        elif start_orbit_number is not None and end_orbit_number is not None:
            if start_orbit_number > end_orbit_number:
                raise InvalidInputError(f"Start orbit ({start_orbit_number}) must be smaller than end orbit ({end_orbit_number}).")
            start_orbit_number = get_validated_orbit_number(start_orbit_number)
            end_orbit_number = get_validated_orbit_number(end_orbit_number)
            return np.arange(start_orbit_number, end_orbit_number + 1).tolist()
    except InvalidInputError as e:
        if logger: logger.exception(e)
        raise
    return None

def get_complete_and_incomplete_orbits(orbit_and_frames: list[tuple[Orbit, Frame]] | None) -> tuple[list[Orbit], dict[Frame, list[Orbit]]] | tuple[None, None]:
    """
    Finds complete orbits (i.e. where all frames are given) and incomplete orbits based on the given list of tuples.

    Args:
        orbit_and_frames: A list of tuples where each tuple contains
                          an orbit number (int) and a frame ID (str).

    Returns:
        tuple:
        - A list of complete orbits.
        - A dictionary where each key is a frame ID, and the value is a list of orbits assigned to that frame.
    """
    if not isinstance(orbit_and_frames, list): return None, None
    if len(orbit_and_frames) == 0: return None, None
    
    orbit_numbers: list[Orbit] = [oaf[0] for oaf in orbit_and_frames]
    frame_ids: list[Frame] = [oaf[1] for oaf in orbit_and_frames]

    df = pd.DataFrame(dict(orbit_number=orbit_numbers, frame_id=frame_ids))

    df_frames_per_orbit_lookup = df.groupby('orbit_number', as_index=False).agg({
        'frame_id': lambda x: ''.join(sorted(''.join(x)))
    })
    mask_complete_orbits = df_frames_per_orbit_lookup['frame_id'] == 'ABCDEFGH'
    complete_orbits = df_frames_per_orbit_lookup.loc[mask_complete_orbits]['orbit_number'].tolist()
    incomplete_orbits = df_frames_per_orbit_lookup.loc[~mask_complete_orbits]['orbit_number'].tolist()
    df_incomplete_orbits = df.loc[df['orbit_number'].isin(incomplete_orbits)]
    df_orbits_per_frame_lookup = df_incomplete_orbits.groupby('frame_id').agg({
        'orbit_number': list
    })
    incomplete_orbits_frame_map = df_orbits_per_frame_lookup.to_dict()['orbit_number']

    return complete_orbits, incomplete_orbits_frame_map

def format_orbit_and_frame(orbit_number: Orbit, frame_id: Frame) -> OrbitAndFrame:
    """Formats orbit number and frame ID  to combined 6 character string (e.g. 01234A)"""
    return str(orbit_number).zfill(5) + frame_id.upper()

def get_counter_message(counter: int | None = None,
                        total_count: int | None = None) -> tuple[str, int]:
    """Creates a formatted counter displaying current and total count (e.g. like this [ 7/10])."""
    max_count_digits = len(str(total_count))
    count_msg = ''
    if counter is not None and total_count is not None:
        count_msg += '[' + str(counter).rjust(max_count_digits) + '/' + str(total_count).rjust(max_count_digits) + ']'
    elif counter is not None:
        count_msg += '[' + str(counter).rjust(max_count_digits) + ']'
    return count_msg, max_count_digits    

def unzip_file(filepath: str,
               delete: bool = False,
               delete_on_error: bool = False,
               counter: int | None = None,
               total_count: int | None = None,
               logger: Logger | None = None) -> bool:
    """
    Extracts file and optionally deletes the original ZIP file upon success or error.

    Args:
        filepath (str): The path to the ZIP file to be extracted.
        delete (bool, optional): If True, the original ZIP file is deleted after extraction. Defaults to False.
        delete_on_error (bool, optional): If True, the ZIP file is deleted if an error occurs during extraction. Defaults to False.
        counter (int or None, optional): A counter to track progress during extraction. Defaults to None.
        total_count (int or None, optional): The total number of files to extract, used for progress tracking. Defaults to None.
        logger (Logger or None, optional): A logger instance to log progress and errors. Defaults to None.

    Returns:
        bool: True if the extraction was successful, False otherwise.
    """
    count_msg, _ = get_counter_message(counter = counter, total_count = total_count)

    if not os.path.exists(filepath):
        if logger: logger.info(f' {count_msg} File not found: <{filepath}>')
        return False

    if logger: console_exclusive_info(f' {count_msg} Extracting...', end='\r')
    new_filepath = os.path.join(os.path.dirname(filepath),
                                os.path.basename(filepath).split('.')[0])
    try:
        with ZipFile(filepath, 'r') as zip_file:
            zip_file.extractall(path=new_filepath)
    except BadZipFile as e:
        if delete_on_error:
            os.remove(filepath)
            if logger: logger.info(f' {count_msg} Unzip failed! ZIP-file was deleted.')
        else:
            if logger: logger.info(f' {count_msg} Unzip failed! <{filepath}>')
        return False

    if delete:
        os.remove(filepath)
        if logger: logger.info(f' {count_msg} File extracted and ZIP-file deleted. (see <{new_filepath}>)')
    else:
        if logger: logger.info(f' {count_msg} File extracted. (see <{new_filepath}>)')
    
    return True

def format_datetime_string(datetime_string: str, logger: Logger | None = None) -> str:
    """Formats time string and raises ValueError if unsuccessful."""
    try:
        timestamp = pd.Timestamp(datetime_string)
        if timestamp.tzinfo is None:
            timestamp = timestamp.tz_localize('UTC')
        return timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
    except ValueError as e:
        msg = f"Given time string '{datetime_string}' is not valid. Here is the original error:"
        if logger: logger.exception(f"{msg}\n{e}")
        raise 

def get_product_type_and_version_from_string(input_string: str, logger: Logger | None = None) -> tuple[str, str]:
    """Returns a tuple of formatted product name and baseline strings (allows short names as input, e.g. 'ANOM:AA' -> ('ATL_NOM_1B', 'AA'))."""
    product_name_input = input_string.replace(' ','').replace('-','').replace('_','').lower()
    product_baseline = None
    tmp = product_name_input.split(':')
    if len(tmp) == 2:
        product_baseline = tmp[1].upper()
        if re.fullmatch('[A-Z]{2}', product_baseline) is None:
            exception_msg = f"Product version in '{input_string}' is not valid. Please specify the product version by giving the two-letter processor baseline after the colon (e.g. ':AC')."
            if logger: logger.exception(exception_msg)
            raise InvalidInputError(exception_msg)
        product_name_input = tmp[0]

    file_types = [
        # ATLID level 1b
        'ATL_NOM_1B',
        'ATL_DCC_1B',
        'ATL_CSC_1B',
        'ATL_FSC_1B',
        # MSI level 1b
        'MSI_NOM_1B',
        'MSI_BBS_1B',
        'MSI_SD1_1B',
        'MSI_SD2_1B',
        # BBR level 1b
        'BBR_NOM_1B',
        'BBR_SNG_1B',
        'BBR_SOL_1B',
        'BBR_LIN_1B',
        # CPR level 1b
        'CPR_NOM_1B', # JAXA product
        # MSI level 1c
        'MSI_RGR_1C',
        # level 1d
        'AUX_MET_1D',
        'AUX_JSG_1D',
        # ATLID level 2a
        'ATL_FM__2A',
        'ATL_AER_2A',
        'ATL_ICE_2A',
        'ATL_TC__2A',
        'ATL_EBD_2A',
        'ATL_CTH_2A',
        'ATL_ALD_2A',
        'ATL_CLA_2A', # JAXA product
        # MSI level 2a
        'MSI_CM__2A',
        'MSI_COP_2A',
        'MSI_AOT_2A',
        'MSI_CLP_2A', # JAXA product
        # CPR level 2a
        'CPR_FMR_2A',
        'CPR_CD__2A',
        'CPR_TC__2A',
        'CPR_CLD_2A',
        'CPR_APC_2A',
        'CPR_ECO_2A', # JAXA product
        'CPR_CLP_2A', # JAXA product
        # ATLID-MSI level 2b
        'AM__MO__2B',
        'AM__CTH_2B',
        'AM__ACD_2B',
        # ATLID-CPR level 2b
        'AC__TC__2B',
        'AC__CLP_2B', # JAXA product
        # BBR-MSI-(ATLID) level 2b
        'BM__RAD_2B',
        'BMA_FLX_2B',
        # ATLID-CPR-MSI level 2b
        'ACM_CAP_2B',
        'ACM_COM_2B',
        'ACM_RT__2B',
        'ACM_CLP_2B', # JAXA product
        # ATLID-CPR-MSI-BBR
        'ALL_DF__2B',
        'ALL_3D__2B',
        'ALL_RAD_2B', # JAXA product
        # Orbit data
        'MPL_ORBSCT', # Orbit scenario file 
        'AUX_ORBPRE', # Predicted orbit file
        'AUX_ORBRES', # Restituted/reconstructed orbit file
    ]

    short_names = []

    for file_type in file_types:
        long_name = file_type.replace('_', '').lower()
        medium_name = long_name[0:-2]
        short_name = medium_name
        string_replacements = [('atl', 'a'), ('msi', 'm'), ('bbr', 'b'), ('cpr', 'c'), ('aux', 'x')]
        for old_string, new_string in string_replacements:
            short_name = short_name.replace(old_string, new_string)
        
        expected_inputs = [long_name, medium_name, short_name]

        if 'ALL_' == file_type[0:4]:
            alternative_long_name = 'acmb' + long_name[3:]
            alternative_short_name = 'acmb' + short_name[3:]
            expected_inputs.extend([alternative_long_name, alternative_short_name])

        if product_name_input.lower() in expected_inputs:
            if product_baseline is not None:
                return file_type, product_baseline
            else:
                return file_type, 'latest'
        
        short_names.append(short_name.upper())

    msg = ''
    msg2 = ''
    for i in range(len(file_types)):
        if i % 6 == 0:
            msg += '\n' + file_types[i]
            msg2 += '\n' + short_names[i]
        else:
            msg += '\t' + file_types[i]
            msg2 += '\t' + short_names[i]

    exception_msg = f'The user input "{input_string}" is either not a valid product name or not supported by this function.\n{msg}\n\nor use the respective short hands (additional non letter characters like - or _ are also allowed, e.g. A-NOM):\n{msg2}'
    if logger: logger.exception(exception_msg)
    raise InvalidInputError(exception_msg)

def get_applicable_collection_list(product_type: str) -> list[str]:
    """Returns a filtered list of the collections in which the specified product is stored."""
    jaxa_l2_products = ['ATL_CLA_2A', 'CPR_ECO_2A', 'CPR_CLP_2A', 'MSI_CLP_2A', 'AC__CLP_2B', 'ACM_CLP_2B', 'ALL_RAD_2B']
    collection_list = [
        'EarthCAREL0L1Products',
        'EarthCAREL1InstChecked',
        'EarthCAREL1Validated',
        'EarthCAREL2Products',
        'EarthCAREL2InstChecked',
        'EarthCAREL2Validated',
        'JAXAL2Products',
        'JAXAL2InstChecked',
        'JAXAL2Validated',
        'EarthCAREAuxiliary',
        'EarthCAREXMETL1DProducts10',
    ]
    if product_type in ['AUX_MET_1D']:
        collection_list = [
            'EarthCAREL0L1Products',
            'EarthCAREL1InstChecked',
            'EarthCAREXMETL1DProducts10',
        ]
    elif product_type.split('_')[-1] in ['1B', '1C', '1D']:
        collection_list = [
            'EarthCAREL0L1Products',
            'EarthCAREL1InstChecked',
            'EarthCAREL1Validated',
        ]
    elif product_type in jaxa_l2_products:
        collection_list = [
            'JAXAL2Products',
            'JAXAL2InstChecked',
            'JAXAL2Validated',
        ]
    elif product_type.split('_')[-1] in ['2A', '2B']:
        collection_list = [
            'EarthCAREL2Products',
            'EarthCAREL2InstChecked',
            'EarthCAREL2Validated',
        ]
    elif product_type.split('_')[-1] in ['ORBSCT', 'ORBPRE', 'ORBRES']:
        collection_list = ['EarthCAREAuxiliary']
    return collection_list

def get_api_request(url_template: str,
                    opensearch_request_parameters: dict,
                    msg_prefix: str | None = None,
                    logger: Logger | None = None) -> str:
    "Substitutes OpenSearch request parameters in given template"
    opensearch_namespace = 'os:'

    # Parameter substitution
    for os_param in opensearch_request_parameters:
        url_template, num_substitutions_made = re.subn(r'\{' + os_param + r'.*?\}', opensearch_request_parameters[os_param] , url_template)
        if (num_substitutions_made < 1):
            if (':' in os_param):
                if logger: logger.warning("Parameter " + os_param + " not found in template.")
            else:
                # Fall back to opensearch_namespace if no namespace provided
                url_template, num_substitutions_made = re.subn(r'\{' + opensearch_namespace + os_param + r'.*?\}', opensearch_request_parameters[os_param] , url_template)
                if (num_substitutions_made < 1):
                    if logger: logger.warning("Parameter " + opensearch_namespace + os_param + " not found in template.")   

    # Remove empty parameters (field-value pairs, e.g. '&bbox={geo:box?}')
    url_template = re.sub(r'&?[a-zA-Z]*=\{.*?\}', '' , url_template)
    # Remove remnants of partially removed parameters (e.g. '/{time:end?}' which originally was part of '&datetime={time:start?}/{time:end?}')
    url_template = re.sub(r'.?\{.*?\}', '' , url_template)

    # Correct list charecters
    url_template = url_template.replace('[', '{').replace(']', '}')

    if logger:
        if msg_prefix is None: msg_prefix = ''
        logger.debug(f"{msg_prefix}API request: <{url_template}>")

    return url_template

def safe_parse_timestamp(timestamp: str) -> pd.Timestamp:
    """Converts string to valid pandas.Timestamp, returns min timestamp on error."""
    try:
        return pd.to_datetime(timestamp, errors="raise")
    except (DateParseError, ValueError):
        return pd.Timestamp.min

def get_product_info_from_path(filepath: str) -> dict[str, str | int | pd.Timestamp]:
    """Gathers product information contained in it's file name."""
    filename = os.path.basename(filepath).split('.')[0]
    if len(filename) < 60:
        frame_id = '-'
        orbit_number = -1
        orbit_and_frame = '-'
    else:
        frame_id = filename[59]
        orbit_number = int(filename[54:59])
        orbit_and_frame = str(orbit_number).zfill(5) + frame_id
    
    product_name = filename[9:19]

    filename_info: dict[str, str | int | pd.Timestamp] = dict(
        filepath = filepath,
        dirpath = os.path.dirname(filepath),
        filename = filename,
        mission_id = filename[0:3],
        agency = filename[4],
        latency_indicator = filename[5],
        product_baseline = filename[6:8],
        file_category = filename[9:13],
        semantic_descriptor = filename[13:17],
        product_level = filename[17:19],
        sensing_start_time = safe_parse_timestamp(filename[20:36]),
        processing_start_time = safe_parse_timestamp(filename[37:53]),
        orbit_number = orbit_number,
        frame_id = frame_id,
        orbit_and_frame = orbit_and_frame,
        product_name = product_name,
    )

    return filename_info

def get_product_sub_dirname(product_name: str) -> str:
    """Returns level subfolder name of given product name."""
    if product_name in ['AUX_JSG_1D', 'AUX_MET_1D']:
        sub_dirname = SUBDIR_NAME_AUX_FILES
    elif product_name in ['MPL_ORBSCT', 'AUX_ORBPRE', 'AUX_ORBRES']:
        sub_dirname = SUBDIR_NAME_ORB_FILES
    elif '0' in product_name.lower():
        sub_dirname = SUBDIR_NAME_L0__FILES
    elif '1b' in product_name.lower():
        sub_dirname = SUBDIR_NAME_L1B_FILES
    elif '1c' in product_name.lower():
        sub_dirname = SUBDIR_NAME_L1C_FILES
    elif '2a' in product_name.lower():
        sub_dirname = SUBDIR_NAME_L2A_FILES
    elif '2b' in product_name.lower():
        sub_dirname = SUBDIR_NAME_L2B_FILES
    return sub_dirname

def ensure_single_zip_extension(filename):
    """Returns given file name with a single .ZIP extension (e.g. 'file.ZIP.zip' -> 'file.ZIP')."""
    base_name, ext = os.path.splitext(filename)
    while ext.lower() == '.zip':
        base_name, ext = os.path.splitext(base_name)
    return base_name + '.ZIP'

def get_local_product_dirpath(dirpath_local, filename, create_subdirs=True):
    """Creates local path to file."""
    if create_subdirs:
        row = get_product_info_from_path(filename)

        product_name = row['product_name']
        year = str(row['sensing_start_time'].year).zfill(4)
        month = str(row['sensing_start_time'].month).zfill(2)
        day = str(row['sensing_start_time'].day).zfill(2)

        sub_dirname = get_product_sub_dirname(product_name)
        product_dirpath_local = os.path.join(dirpath_local, sub_dirname, product_name, year, month, day)
    else:
        product_dirpath_local = dirpath_local
    return product_dirpath_local

def download(
    dataframe: pd.DataFrame,
    username: str,
    password: str,
    download_directory: str,
    is_overwrite: bool,
    is_unzip: bool,
    is_delete: bool,
    is_create_subdirs: bool,
    logger: Logger | None = None
):
    """
    Download files based on the provided dataframe of OpenSearch query results.

    Args:
        dataframe (pd.DataFrame): DataFrame containing the OpenSearch query results (i.e. file URLs and names).
        username (str): OADS authentication username.
        password (str): OADS authentication password.
        download_directory (str): Target directory for storing downloaded files.
        is_overwrite (bool): If True, overwrite existing files.
        is_unzip (bool): If True, extract downloaded archives.
        is_delete (bool): If True, delete archives after extraction.
        is_create_subdirs (bool): If True, place files in subfolder structure.
        logger (Logger | None, optional): Logger instance for logging messages.

    Returns:
        None
    """
    total_count = len(dataframe)
    counter = 1
    download_counter = 0
    unzip_counter = 0
    download_sizes = []
    download_speeds = []
    for server, df_group in dataframe.groupby('server'):
        proxies: dict = {}

        oads_hostname = server
        if logger: logger.info(f"Selecting dissemination service: {oads_hostname}")
        eoiam_idp_hostname = "eoiam-idp.eo.esa.int"

        # Requesting access to the OADS server storing the products
        access_response = requests.get(f"https://{oads_hostname}/oads/access/login", proxies=proxies)
        validate_request_response(access_response, logger=logger)

        # Extracting the cookies from the response
        access_response_cookies = access_response.cookies
        for r in access_response.history:
            access_response_cookies = requests.cookies.merge_cookies(access_response_cookies, r.cookies)
        tree = html.fromstring(access_response.content)

        # Extracting the sessionDataKey from the the response
        sessionDataKey = tree.findall(".//input[@name = 'sessionDataKey']")[0].attrib["value"]

        # Defining login request
        post_data = {
            "tocommonauth": "true",
            "username": username,
            "password": password,
            "sessionDataKey": sessionDataKey,
        }

        # Sending the login request to the authentication platform
        auth_url = f"https://{eoiam_idp_hostname}/samlsso"
        auth_response = requests.post(url=auth_url,
                                      data=post_data,
                                      cookies=access_response_cookies,
                                      proxies=proxies)
        validate_request_response(auth_response, logger=logger)

        # Parsing the response from authentication platform
        tree = html.fromstring(auth_response.content)
        # responseView = BeautifulSoup(auth_response.text, 'html.parser')
        # if logger: logger.debug(responseView)

        # Extracting the variables needed to redirect from a successful authentication to OADS
        try:
            relayState = tree.findall(".//input[@name='RelayState']")[0].attrib["value"]
            samlResponse = tree.findall(".//input[@name='SAMLResponse']")[0].attrib["value"]
        except IndexError as e:
            exception_msg = "OADS did not responde as expected. Check your configuration file for valid a username and password."
            if logger: logger.exception(exception_msg)
            raise BadResponseError(exception_msg)

        # Defining the SAML redirection request to OADS
        post_data = {
            "RelayState": relayState,
            "SAMLResponse": samlResponse,
        }

        # Sending the SAML redirection request to OADS
        saml_redirect_url = tree.findall(".//form[@method='post']")[0].attrib["action"]
        saml_response = requests.post(url=saml_redirect_url,
                                      data=post_data,
                                      proxies=proxies)
        validate_request_response(saml_response, logger=logger)

        saml_response_cookies = saml_response.cookies
        for r in saml_response.history:
            saml_response_cookies = requests.cookies.merge_cookies(saml_response_cookies, r.cookies)

        # Downloading Products
        for index, row in df_group.iterrows():
            count_msg, _ = get_counter_message(counter=counter, total_count=total_count)

            success = False

            # Extracting the filename from the download link
            file_name = (row['download_url']).split("/")[-1]
            product_dirpath = get_local_product_dirpath(download_directory, file_name, create_subdirs=is_create_subdirs)
            # Make sure the local download_directory exists (if not create it)
            if not os.path.exists(product_dirpath): 
                os.makedirs(product_dirpath)
            # Some files may be missing zip file extension so we need to fix them
            file_name = ensure_single_zip_extension(file_name)
            zip_file_path = os.path.join(product_dirpath, file_name)
            file_path = zip_file_path[0:-4]

            if logger: logger.info(f"*{count_msg} Starting: {file_name[0:-4]}")

            # Defining the download URL
            file_download_url = row['download_url']

            for attempt in range(MAX_DOWNLOAD_ATTEMPTS_PER_FILE):
                if attempt > 0:
                    if logger: logger.info(f" {count_msg} Restarting (starting try {attempt + 1} of max. {MAX_DOWNLOAD_ATTEMPTS_PER_FILE}).")
                      
                success = True

                # Check existing files
                zip_file_exists = os.path.exists(zip_file_path)
                file_exists = os.path.exists(file_path)

                # Decide if file will be downloaded and extracted
                try_download = is_overwrite or (not zip_file_exists and not file_exists)
                try_unzip = is_unzip and (is_overwrite or not file_exists)

                if not try_download:
                    if is_unzip:
                        if logger: logger.info(f" {count_msg} Skip file download.")
                    else:
                        if logger: logger.info(f" {count_msg} Skip file download. (see <{zip_file_path}>)")
                if not try_unzip:
                    if logger: logger.info(f" {count_msg} Skip file unzip. (see <{file_path}>)")
                if not try_download and not try_unzip:
                    counter += 1
                    break

                # Delete unnessecary zip files
                if is_delete and file_exists and zip_file_exists:
                    os.remove(zip_file_path)
                    zip_file_exists = False

                # Overwrite files
                if zip_file_exists and is_overwrite:
                    os.remove(zip_file_path)
                    zip_file_exists = False
                if file_exists and is_overwrite:
                    os.remove(file_path)
                    file_exists = False

                # Download zip file
                if try_download:
                    try:
                        # Requesting the product download
                        if logger: logger.debug(f" {count_msg} Requesting: {file_download_url}")
                        file_download_response = requests.get(file_download_url, 
                                                              cookies = saml_response_cookies,
                                                              proxies = proxies, 
                                                              stream = True)
                        validate_request_response(file_download_response, logger=logger)
                        
                        with open(zip_file_path, "wb") as f:
                            total_length_str = file_download_response.headers.get('content-length')
                            if not isinstance(total_length_str, str):
                                f.write(file_download_response.content)
                            else:
                                current_length = 0
                                total_length = int(total_length_str)
                                start_time = time.time()
                                progress_bar_length = 30
                                for data in file_download_response.iter_content(chunk_size=CHUNK_SIZE_BYTES): 
                                    current_length += len(data)
                                    f.write(data)
                                    done = int(progress_bar_length * current_length / total_length)
                                    time_elapsed = (time.time() - start_time)
                                    time_estimated = (time_elapsed/current_length) * total_length
                                    time_left = time.strftime("%H:%M:%S", time.gmtime(int(time_estimated - time_elapsed)))
                                    progress_bar = f"[{'#' * done}{'-' * (progress_bar_length - done)}]"
                                    progress_percentage = f"{str(int((current_length / total_length) * 100)).rjust(3)}%"
                                    elapsed_time = time.time() - start_time
                                    size_done = current_length / 1024 / 1024
                                    size_total = total_length / 1024 / 1024
                                    speed = size_done / elapsed_time if elapsed_time > 0 else 0  # MB/s
                                    if logger: console_exclusive_info(f"\r {count_msg} {progress_percentage} {progress_bar} {time_left} - {speed:.2f} MB/s - {size_done:.2f}/{size_total:.2f} MB", end='\r')
                                time_taken = time.strftime("%H:%M:%S", time.gmtime(int(time.time() - start_time)))
                                if logger: logger.info(f" {count_msg} Download completed ({time_taken} - {speed:.2f} MB/s - {size_done:.2f}/{size_total:.2f} MB)                   ")
                                download_sizes.append(size_total)
                                download_speeds.append(speed)
                                download_counter += 1
                    except requests.exceptions.RequestException as e:
                        is_error_403_forbidden = False
                        if e.response is not None:  # Ensure response exists
                            is_error_403_forbidden = e.response.status_code == 403
                        if is_error_403_forbidden:
                            attempt = MAX_DOWNLOAD_ATTEMPTS_PER_FILE
                            if logger:
                                logger.error(f"DOWNLOAD FAILED: {e}")
                                logger.error(f"Make sure that you only use OADS collections that you are allowed to access in your config.toml (see section 'Setup' in README)!")
                        else:
                            if logger: logger.info(f" {count_msg} DOWNLOAD FAILED for attempt {attempt + 1} of {MAX_DOWNLOAD_ATTEMPTS_PER_FILE}: {e}")
                            time.sleep(2)  # Wait for 2 seconds before retrying

                    download_success = os.path.exists(zip_file_path)
                    success &= download_success

                # Unzip zip file
                if try_unzip:
                    success = unzip_file(zip_file_path,
                                         delete=is_delete,
                                         delete_on_error=True,
                                         total_count=total_count,
                                         counter=counter,
                                         logger=logger)
                    unzip_success = os.path.exists(file_path)
                    if unzip_success: unzip_counter += 1
                    success &= unzip_success

                if success:
                    counter += 1
                    break

        # Logout of authentication platform and OADS
        with requests.get(f'https://{oads_hostname}/oads/Shibboleth.sso/Logout', proxies=proxies, stream=True) as _: pass
        with requests.get(f'https://{eoiam_idp_hostname}/Shibboleth.sso/Logout', proxies=proxies, stream=True) as _: pass
    
    total_download_size = 0 if len(download_sizes) == 0 else np.sum(download_sizes)
    mean_download_speed = 0 if len(download_speeds) == 0 else np.mean(download_speeds)

    return download_counter, unzip_counter, mean_download_speed, total_download_size

def split_list_into_chunks(lst: list, size: int) -> list[list]:
    """Splits a list into chunks or sublists each containing at most N elements"""
    iterator = iter(lst)
    return [list(islice(iterator, size)) for _ in range((len(lst) + size - 1) // size)]

def encode_url(url: str) -> str:
    """Encode the URL, including its query string."""
    split_parsed_url = urlp.urlsplit(url)
    encoded_query = urlp.quote(split_parsed_url.query, safe="=&,/:") # Keep separators
    return urlp.urlunsplit((
        split_parsed_url.scheme,
        split_parsed_url.netloc,
        split_parsed_url.path,
        encoded_query,
        split_parsed_url.fragment
    ))

def get_df(
    url_product_search_query: str,
    logger: Logger | None = None,
) -> pd.DataFrame:
    """Performs given search request and returns results as `pandas.Dataframe`."""
    # Ensures that URL is properly encoded
    url_product_search_query = url_product_search_query.replace('[', '{').replace(']', '}')
    url_product_search_query = encode_url(url_product_search_query)

    # Performs the request
    response = get_request(url_product_search_query, logger=logger)
    data_product_search_query = json.loads(response.text)

    # Creates dataframe from result
    data = []
    for d in data_product_search_query['features']:
        id = d['id']
        server = urlp.urlparse(d['assets']['enclosure']['href']).netloc
        download_url = d['assets']['enclosure']['href']
        data.append((id, server, download_url))

    df = pd.DataFrame(data, columns=['id', 'server', 'download_url'])
    return df

def get_product_list_json(url_items: str,
                     product_id_text: str | None = None,
                     sort_by_text: str | None = None,
                     num_results_text: str | None = "1000",
                     start_time_text: str | None = None,
                     end_time_text: str | None = None,
                     poi_text: str | None = None,
                     bbox_text: str | None = None,
                     illum_angle_text: str | None = None,
                     frame_text: str | None = None,
                     orbit_number_text: str | None = None,
                     instrument_text: str | None = None,
                     productType_text: str | None = None,
                     productVersion_text: str | None = None,
                     orbitDirection_text: str | None = None,
                     radius_text: str | None = None,
                     lat_text: str | None = None,
                     lon_text: str | None = None,
                     msg_prefix: str | None = '',
                     logger: Logger | None = None) -> pd.DataFrame:
    """
    Performs a product search based on given search criteria.

    Args:
        url_items (str): Base items URL that gets extended by other given search parameters.
        msg_prefix (str, optional): Prefix for log messages. Defaults to an empty string.
        logger (Logger | None, optional): Logger instance for logging. Defaults to None.

    Returns:
        pd.DataFrame: DataFrame containing found products.
    """
    # Define the OpenSearch request parameters
    request_parameters = ""
    if num_results_text: request_parameters += f"&limit={num_results_text}"
    if poi_text: request_parameters += f"&geometry={poi_text}"
    if bbox_text: request_parameters += f"&bbox={bbox_text}"
    if start_time_text and end_time_text:
        request_parameters += f"&datetime={start_time_text}/{end_time_text}"
    elif start_time_text:
        request_parameters += f"&datetime={start_time_text}/"
    elif end_time_text:
        request_parameters += f"&datetime=/{end_time_text}"
    if product_id_text: request_parameters += f"&uid={product_id_text}"
    if sort_by_text: request_parameters += f"&sortKeys={sort_by_text}"
    if illum_angle_text: request_parameters += f"&illuminationElevationAngle={illum_angle_text}"
    if frame_text: request_parameters += f"&frame={frame_text}"
    if orbit_number_text: request_parameters += f"&orbitNumber={orbit_number_text}"
    if instrument_text: request_parameters += f"&instrument={instrument_text}"
    if productType_text: request_parameters += f"&productType={productType_text}"
    if productVersion_text: request_parameters += f"&productVersion={productVersion_text}"
    if orbitDirection_text: request_parameters += f"&orbitDirection={orbitDirection_text}"
    if radius_text: request_parameters += f"&radius={radius_text}"
    if lat_text: request_parameters += f"&lat={lat_text}"
    if lon_text: request_parameters += f"&lon={lon_text}"

    request_url = f"{url_items}?{request_parameters}"
    if logger: logger.debug(f"Constructed search request URL: {request_url}")

    # Extract the results into a dataframe 
    dataframe = get_df(request_url, logger=logger)

    return dataframe

def drop_duplicate_files(df, filename_column):
    """Drops duplicate files in given dataframe."""
    if len(df) == 0:
        return df

    # Keep only the latest file (i.e. with latest processing_start_time)
    def extract_info(filename):
        info = get_product_info_from_path(filename)
        return info['product_name'], info['sensing_start_time'], info['processing_start_time']
    df[['product_name', 'sensing_start_time', 'processing_start_time']] = df[filename_column].apply(extract_info).apply(pd.Series)
    df = df.sort_values(by=['product_name', 'sensing_start_time', 'processing_start_time'], ascending=[True, True, False])
    df = df.drop_duplicates(subset=['product_name', 'sensing_start_time'], keep='first')
    df = df.drop(columns=['product_name', 'sensing_start_time', 'processing_start_time'])

    return df.reset_index(drop=True)

def get_frame_range(start_frame_id: str, end_frame_id: str) -> list[str]:
    """Returns list of frames in order of selected range (e.g. A-D -> ABCD and D-A -> DEFGHA)."""
    start_idx = FRAMES.index(start_frame_id)
    end_idx = FRAMES.index(end_frame_id)
    if end_idx < start_idx:
        end_idx = end_idx + NUM_FRAMES
    frame_id_range = [FRAMES[idx % NUM_FRAMES] for idx in np.arange(start_idx, end_idx + 1)]
    return frame_id_range

def get_parsed_arguments() -> dict:
    """Defines the CLI and parses all arguments given by the user."""
    parser = argparse.ArgumentParser(
                    prog=PROGRAM_NAME,
                    description=f"{__description__}\n\n{SETUP_INSTRUCTIONS}",
                    formatter_class=RawTextHelpFormatter)
    parser.add_argument("product_type",
                        type = str,
                        nargs = '*',
                        help = "A list of EarthCARE product names (e.g. ANOM or ATL-NOM-1B, etc.).\nYou can also specify the product version by adding a colon and the two-letter\nprocessor baseline after the name (e.g. ANOM:AD).")
    parser.add_argument("-d", "--data_directory",
                        type = str,
                        default = None,
                        help = "The local root directory where products will be downloaded to")
    parser.add_argument("-o", "--orbit_number",
                        type = int,
                        nargs = '*',
                        default = None,
                        help = "A list of EarthCARE orbit numbers (e.g. 981)")
    parser.add_argument("-so", "--start_orbit_number",
                        type = int,
                        default = None,
                        help = "Start of orbit number range (e.g. 981). Can only be used in combination with option -eo.")
    parser.add_argument("-eo", "--end_orbit_number",
                        type = int,
                        default = None,
                        help = "End of orbit number range (e.g. 986). Can only be used in combination with option -so.")
    parser.add_argument("-f", "--frame_id",
                        type = str,
                        nargs = '*',
                        default = None,
                        help = "A EarthCARE frame ID (i.e. single letters from A to H)")
    parser.add_argument("-oaf", "--orbit_and_frame",
                        type = str,
                        nargs = '*',
                        default = None,
                        help = "A string describing the EarthCARE orbit number and frame (e.g. 00981E)")
    parser.add_argument("-soaf", "--start_orbit_and_frame",
                        type = str,
                        default = None,
                        help = "Start orbit number and frame range (e.g. 00981E). Can only be used in combination with option -eoaf. Can not be used with separate orbit and frame options -o, -so, eo and -f.")
    parser.add_argument("-eoaf", "--end_orbit_and_frame",
                        type = str,
                        default = None,
                        help = "End orbit number and frame range (e.g. 00982B). Can only be used in combination with option -soaf. Can not be used with separate orbit and frame options -o, -so, eo and -f.")
    parser.add_argument("-t", "--time",
                        type = str,
                        nargs = '*',
                        default = None,
                        help = 'Search for data containing a specific timestamp (e.g. "2024-07-31 13:45" or 20240731T134500Z)')
    parser.add_argument("-st", "--start_time",
                        type = str,
                        default = None,
                        help = 'Start of sensing time (e.g. "2024-07-31 13:45" or 20240731T134500Z)')
    parser.add_argument("-et", "--end_time",
                        type = str,
                        default = None,
                        help = 'End of sensing time (e.g. "2024-07-31 13:45" or 20240731T134500Z)')
    parser.add_argument("-r", "--radius_search",
                        type = str,
                        nargs = 3,
                        default = None,
                        help = "Perform search around a radius around a point (e.g. 25000 51.35 12.43, i.e. <radius[m]> <latitude> <longitude>)")
    parser.add_argument("-pv", "--product_version",
                        type = str,
                        default = None,
                        help = 'Product version, i.e. the two-letter identifier of the processor baseline (e.g. AC)')
    parser.add_argument("-bbox", "--bounding_box",
                        type = str,
                        nargs = 4,
                        default = None,
                        help = "Perform search inside a bounding box (e.g. 14.9 37.7 14.99 37.78, i.e. <latS> <lonW> <latN> <lonE>)")
    parser.add_argument("--overwrite", action="store_true",
                        help="Overwrite local data (otherwise existing local data will not be downloaded again)")
    parser.add_argument("--no_download", action="store_false",
                        help="Do not download any data")
    parser.add_argument("--no_unzip", action="store_false",
                        help="Do not unzip any data")
    parser.add_argument("--no_delete", action="store_false",
                        help="Do not delete zip files after unzipping them")
    parser.add_argument("--no_subdirs", action="store_false",
                        help="Do not create subdirs like: data_directory/data_level/product_type/year/month/day")
    parser.add_argument("-c", "--path_to_config",
                        type = str,
                        default = None,
                        help = "The path to an OADS credential TOML file (note: if not provided, a file named 'config.toml' is required in the script's folder)")
    parser.add_argument("--debug", action="store_true",
                        help="Shows debug messages in console.")
    parser.add_argument("--no_log", action="store_false",
                        help="Prevents generation of log files.")
    parser.add_argument("-i", "--select_file_at_index",
                        type = int,
                        default = None,
                        help = "Select only one product from the found products list by index for download. You may provide a negative index to start from the last entry (e.g. -1 downloads the last file listed).")
    parser.add_argument("-V", "--version", action="store_true",
                        help="Shows the script's version and exit")
    parser.add_argument("--export_results", action="store_true",
                        help="Writes names of found files to a txt file called 'results.txt'")
    args = parser.parse_args()

    if args.version:
        console_exclusive_info(f"{PROGRAM_NAME} {__version__} (released on {__date__})")
        sys.exit(0)

    return dict(
        product_types=args.product_type,
        path_to_data=args.data_directory,
        timestamps=args.time,
        frame_ids=args.frame_id,
        orbit_numbers=args.orbit_number,
        orbit_and_frames=args.orbit_and_frame,
        start_time=args.start_time,
        end_time=args.end_time,
        radius_search=args.radius_search,
        bounding_box=args.bounding_box,
        is_download=args.no_download,
        is_unzip=args.no_unzip,
        is_delete=args.no_delete,
        is_overwrite=args.overwrite,
        is_create_subdirs=args.no_subdirs,
        product_version=args.product_version,
        path_to_config=args.path_to_config,
        download_idx=args.select_file_at_index,
        start_orbit_number=args.start_orbit_number,
        end_orbit_number=args.end_orbit_number,
        start_orbit_and_frame=args.start_orbit_and_frame,
        end_orbit_and_frame=args.end_orbit_and_frame,
        is_log=args.no_log,
        is_debug=args.debug,
        is_found_files_list_to_txt=args.export_results,
    )

def get_time_queryparams(start_time: str | None,
                         end_time: str | None,
                         timestamps: list[str] | None,
                         logger: Logger | None = None) -> tuple[str | None,
                                                          str | None,
                                                          list[str] | None]:
        """Converts user's time inputs to query parameter strings, that can be used in search requests."""
        start_time_queryparam = None
        if start_time is not None:
            start_time_queryparam = format_datetime_string(start_time, logger=logger)
        
        end_time_queryparam = None
        if end_time is not None:
            end_time_queryparam = format_datetime_string(end_time, logger=logger)
        
        timestamp_queryparams = None
        if timestamps is not None:
            timestamp_queryparams = [format_datetime_string(t, logger=logger) for t in timestamps]
            for ts_queryparam in timestamp_queryparams:
                try:
                    if start_time_queryparam is not None:
                        if ts_queryparam < start_time_queryparam:
                            raise InvalidInputError(f"Timestamp ({ts_queryparam}) must be greater or equal the start time ({start_time}).")
                    if end_time_queryparam is not None:
                        if ts_queryparam > end_time_queryparam:
                            raise InvalidInputError(f"Timestamp ({ts_queryparam}) must be smaller or equal the end time ({end_time}).")
                except InvalidInputError as e:
                    if logger: logger.exception(e)
                    raise
        return start_time_queryparam, end_time_queryparam, timestamp_queryparams

def get_frame_queryparams(frame_ids: list[Frame] | None, logger: Logger | None = None) -> list[Frame] | None:
    """Convert user's frame ID input to query parameters, that can be used in search requests."""
    frame_id_queryparams = None
    if frame_ids is not None:
        frame_id_queryparams = [get_validated_frame_id(f, logger=logger) for f in frame_ids]
        is_all_frames = len([f for f in FRAMES if f not in frame_id_queryparams]) == 0
        if is_all_frames:
            if logger: logger.warning("You used the --frame_id/-f option with all frames (A to H). If you want to download all frame IDs you don't need to use this option.")
    return frame_id_queryparams

def get_orbit_queryparams(start_orbit_number: Orbit | None,
                          end_orbit_number: Orbit | None,
                          orbit_numbers: list[Orbit] | None,
                          logger: Logger | None = None) -> list[Orbit] | None:
    """Convert user's orbit number inputs to query parameters, that can be used in search requests."""
    orbit_number_queryparams: list[Orbit] = []

    if isinstance(orbit_numbers, list):
        orbit_number_queryparams = [int(x) for x in np.append(orbit_number_queryparams, orbit_numbers)]

    orbit_number_range = get_validated_orbit_number_range(start_orbit_number, end_orbit_number, logger=logger)
    if isinstance(orbit_number_range, list):
        orbit_number_queryparams = [int(x) for x in np.append(orbit_number_queryparams, orbit_number_range)]

    if isinstance(orbit_number_queryparams, list):
        orbit_number_queryparams = [int(x) for x in np.sort(np.unique(orbit_number_queryparams))]

    return orbit_number_queryparams

def get_radius_queryparams(radius_search: list[str] | None) -> tuple[str, str, str] | tuple[None, None, None]:
    """Convert user's radius inputs to query parameters, that can be used in search requests."""
    if radius_search is not None:
        radius_queryparam = str(int(radius_search[0]))
        lat_queryparam = str(float(radius_search[1]))
        lon_queryparam = str(float(radius_search[2]))
        return radius_queryparam, lat_queryparam, lon_queryparam
    return None, None, None

def get_bbox_queryparam(bounding_box: list[str] | None) -> str | None:
    """Convert user's bounding box inputs to a query parameter, that can be used in search requests."""
    if bounding_box is not None:
        return ','.join([str(float(x[1])) for x in bounding_box])
    return None

def get_orbit_frame_tuple_list_from_separate_orbit_and_frame_lists(
        orbits: list[Orbit] | None,
        frames: list[Frame] | None
    ) -> list[tuple[Orbit, Frame]] | None:
        """Takes 2 lists of selected orbits and frames, then returns list of tuples where each tuple contains an orbit number (int) and a frame ID (str)."""
        if orbits is None or len(orbits) == 0:
            return None
        if frames is None or len(frames) == 0:
            frames = [f for f in FRAMES]
        new_orbits = [int(x) for x in np.tile(orbits, len(frames))]
        new_frames = [str(x) for x in np.repeat(frames, len(orbits))]
        return list(zip(new_orbits, new_frames))

def get_orbit_frame_tuple_list_from_strings(start_orbit_and_frame: OrbitAndFrame | None,
                                            end_orbit_and_frame: OrbitAndFrame | None,
                                            orbit_and_frames: list[OrbitAndFrame] | None,
                                            orbit_frame_tuple_list: list[tuple[Orbit, Frame]] | None = None,
                                            logger: Logger | None = None) -> list[tuple[Orbit, Frame]] | None:
        """Takes user's orbit_and_frame string inputs, then returns list of tuples where each tuple contains an orbit number (int) and a frame ID (str)."""
        try:
            if (start_orbit_and_frame is None and end_orbit_and_frame is not None):
                raise InvalidInputError(f"End orbit and frame was given ({end_orbit_and_frame}) but start is missing.")
            if (start_orbit_and_frame is not None and end_orbit_and_frame is None):
                raise InvalidInputError(f"Start orbit and frame was given ({start_orbit_and_frame}) but end is missing.")
        except InvalidInputError as e:
            if logger: logger.exception(e)
            raise
        
        orbit_numbers = []
        frame_ids: list[Frame] = []
        if start_orbit_and_frame is not None and end_orbit_and_frame is not None:
            start_orbit_number, start_frame_id = get_validated_orbit_and_frame(start_orbit_and_frame, logger=logger)
            end_orbit_number, end_frame_id = get_validated_orbit_and_frame(end_orbit_and_frame, logger=logger)
            orbit_number_range = np.arange(start_orbit_number, end_orbit_number + 1)
            if len(orbit_number_range) == 1:
                orbit_numbers = [start_orbit_number] * len(frame_ids)
                frame_ids = get_frame_range(start_frame_id, end_frame_id)
            else:
                frame_ids_start = get_frame_range(start_frame_id, 'H')
                orbit_numbers_start = [start_orbit_number] * len(frame_ids_start)

                frame_ids_end = get_frame_range('A', end_frame_id)
                orbit_numbers_end = [end_orbit_number] * len(frame_ids_end)

                orbit_numbers = orbit_numbers_start + orbit_numbers_end
                frame_ids = frame_ids_start + frame_ids_end
                if len(orbit_number_range) >= 3:
                    orbit_numbers_middle = orbit_number_range[1:-1]
                    for o in orbit_numbers_middle:
                        for f in FRAMES:
                            orbit_numbers.append(o)
                            frame_ids.append(f)

        if orbit_and_frames is not None:
            oaf_tuple_list = [get_validated_orbit_and_frame(oaf) for oaf in orbit_and_frames]
            orbit_numbers_given: list[Orbit] = [oaf[0] for oaf in oaf_tuple_list]
            frame_ids_given: list[Frame] = [oaf[1] for oaf in oaf_tuple_list]
            orbit_numbers = orbit_numbers + orbit_numbers_given
            frame_ids = frame_ids + frame_ids_given

        new_orbit_frame_tuple_list = None
        if len(orbit_numbers) > 0:
            new_orbit_frame_tuple_list = list(zip(orbit_numbers, frame_ids))
    
        if orbit_frame_tuple_list is not None and new_orbit_frame_tuple_list is not None:
            return orbit_frame_tuple_list + new_orbit_frame_tuple_list
        
        if orbit_frame_tuple_list is not None:
            return orbit_frame_tuple_list
        
        return new_orbit_frame_tuple_list

def create_list_of_search_requests(product_types: list[str],
                                   product_versions: list[str],
                                   radius_queryparam: str | None,
                                   lat_queryparam: str | None,
                                   lon_queryparam: str | None,
                                   bbox_queryparam: str | None,
                                   start_time_queryparam: str | None,
                                   end_time_queryparam: str | None,
                                   timestamp_queryparams: list[str] | None,
                                   complete_orbits: list[Orbit] | None,
                                   incomplete_orbits_frame_map: dict[Frame, list[Orbit]] | None,
                                   frame_ids: list[Frame] | None) -> list[SearchRequest]:
    """
    Creates a list of search requests based on product types, spatial and temporal 
    query parameters, and orbit/frame information.

    Returns:
        list (list[SearchRequest]): A list of search request objects.
    """
    planned_requests = []
    for product_type, product_version in zip(product_types, product_versions):
        basic_product_queryparams = dict(collection_identifier_list=get_applicable_collection_list(product_type),
                                        product_type=product_type,
                                        product_version=None if product_version == 'latest' else product_version)
        geo_location_queryparams = dict(radius=radius_queryparam,
                                        lat=lat_queryparam,
                                        lon=lon_queryparam,
                                        bbox=bbox_queryparam)
        if start_time_queryparam is None and end_time_queryparam is not None:
            start_time_queryparam = format_datetime_string("2024-05-28T22:20:00Z")
        if start_time_queryparam is not None and end_time_queryparam is None:
            end_time_queryparam = format_datetime_string(str(pd.Timestamp.now()))
        time_queryparams = dict(start_time=start_time_queryparam,
                                end_time=end_time_queryparam)
        
        if timestamp_queryparams is not None:
            for ts_queryparam in timestamp_queryparams:
                new_request = SearchRequest(**basic_product_queryparams,
                                            start_time=ts_queryparam,
                                            end_time=ts_queryparam)
                planned_requests.append(new_request)
        
        if complete_orbits is not None:
            complete_orbits_chunks = split_list_into_chunks(complete_orbits, MAX_NUM_ORBITS_PER_REQUEST)
            for complete_orbits_chunk in complete_orbits_chunks:
                complete_orbits_chunk_queryparam = '[' + ','.join([str(get_validated_orbit_number(o)) for o in complete_orbits_chunk]) + ']'
                new_request = SearchRequest(**basic_product_queryparams,
                                            **geo_location_queryparams,
                                            **time_queryparams,
                                            orbit_number=complete_orbits_chunk_queryparam)
                planned_requests.append(new_request)
        
        if incomplete_orbits_frame_map is not None:
            for frame_id_queryparam, incomplete_orbits in incomplete_orbits_frame_map.items():
                incomplete_orbits_chunks = split_list_into_chunks(incomplete_orbits, MAX_NUM_ORBITS_PER_REQUEST)
                for incomplete_orbits_chunk in incomplete_orbits_chunks:
                    incomplete_orbits_chunk_queryparam = '[' + ','.join([str(get_validated_orbit_number(o)) for o in incomplete_orbits_chunk]) + ']'
                    new_request = SearchRequest(**basic_product_queryparams,
                                                **geo_location_queryparams,
                                                **time_queryparams,
                                                frame_id=frame_id_queryparam,
                                                orbit_number=incomplete_orbits_chunk_queryparam)
                    planned_requests.append(new_request)
        
        if (complete_orbits is None and incomplete_orbits_frame_map is None
            and (start_time_queryparam is not None and end_time_queryparam is not None)):
            if frame_ids is not None:
                for frame_id in frame_ids:
                    new_request = SearchRequest(**basic_product_queryparams,
                                                **geo_location_queryparams,
                                                **time_queryparams,
                                                frame_id=str(frame_id))
                planned_requests.append(new_request)
            else:
                new_request = SearchRequest(**basic_product_queryparams,
                                            **geo_location_queryparams,
                                            **time_queryparams)
                planned_requests.append(new_request)
    return planned_requests

def main(
    product_types: list[str],
    path_to_data: str | None,
    timestamps: list[str] | None,
    frame_ids: list[Frame] | None,
    orbit_numbers: list[Orbit] | None,
    orbit_and_frames: list[OrbitAndFrame] | None,
    start_time: str | None,
    end_time: str | None,
    radius_search: list[str] | None,
    bounding_box: list[str] | None,
    is_download: bool,
    is_unzip: bool,
    is_delete: bool,
    is_overwrite: bool,
    is_create_subdirs: bool,
    product_version: str | None,
    path_to_config: str | None,
    download_idx: int | None,
    start_orbit_number: Orbit | None,
    end_orbit_number: Orbit | None,
    start_orbit_and_frame: Frame | None,
    end_orbit_and_frame: Frame | None,
    is_log: bool,
    is_debug: bool,
    is_found_files_list_to_txt: bool,
    logger: Logger,
):
    raw_user_inputs = locals()

    time_start_script = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Welcome message
    if logger:
        log_heading(f"EARTHCARE OADS DOWNLOAD SCRIPT (v{__version__})", logger, is_mayor=True)
        logger.debug("User inputs:")
        for k, v in raw_user_inputs.items():
            logger.debug(f' - {k}: {v}')

    # Validate and format user inputs
    selected_index = get_validated_selected_index(download_idx, logger=logger)
    validate_combination_of_given_orbit_and_frame_range_inputs(
        start_orbit_and_frame, end_orbit_and_frame,
        start_orbit_number, end_orbit_number,
        orbit_numbers, frame_ids,
        logger=logger
    )

    type_version_tuples = [get_product_type_and_version_from_string(pn, logger=logger) for pn in product_types]
    product_types = [x[0] for x in type_version_tuples]
    product_versions = [x[1] for x in type_version_tuples]
    # If option '--product_version' is used, all products without explicitly specified baseline in its name (eg. ANOM:AC) will be set to this 
    if product_version is not None:
        product_versions = [product_version if pv == 'latest' else pv for pv in product_versions]
    
    # Convert user's inputs to query parameters, that can be used in search requests
    # Temporal inputs
    start_time_queryparam, end_time_queryparam, timestamp_queryparams = get_time_queryparams(
        start_time, 
        end_time, 
        timestamps
    )
    # Spatial inputs
    radius_queryparam, lat_queryparam, lon_queryparam = get_radius_queryparams(radius_search)
    bbox_queryparam = get_bbox_queryparam(bounding_box)
    # Orbit and frame inputs
    frame_id_queryparams = get_frame_queryparams(frame_ids)
    orbit_number_queryparams = get_orbit_queryparams(
        start_orbit_number,
        end_orbit_number,
        orbit_numbers,
        logger=logger
    )
    orbit_frame_tuple_list = get_orbit_frame_tuple_list_from_separate_orbit_and_frame_lists(
        orbit_number_queryparams,
        frame_id_queryparams
    )
    orbit_frame_tuple_list = get_orbit_frame_tuple_list_from_strings(
        start_orbit_and_frame,
        end_orbit_and_frame,
        orbit_and_frames,
        orbit_frame_tuple_list,
        logger=logger
    )
    complete_orbits, incomplete_orbits_frame_map = get_complete_and_incomplete_orbits(orbit_frame_tuple_list)

    # Construct series of search requests
    planned_requests = create_list_of_search_requests(
        product_types, product_versions,
        radius_queryparam, lat_queryparam, lon_queryparam, bbox_queryparam,
        start_time_queryparam, end_time_queryparam, timestamp_queryparams,
        complete_orbits, incomplete_orbits_frame_map, frame_ids
    )

    if logger:
        console_exclusive_info()
        log_heading(f"PART 1 - Search products", logger)
        console_exclusive_info()

    # Read credentials
    if path_to_config is None:
        path_to_script = os.path.abspath(__file__)
        path_to_script_dir = os.path.dirname(path_to_script)
        path_to_config = os.path.join(path_to_script_dir, 'config.toml')
        if logger: logger.info(f"Setting path_to_config to <{path_to_config}>")
    username = ""
    password = ""
    try:
        if os.path.exists(path_to_config):
            with open(path_to_config, 'rb') as f:
                file = tomllib.load(f)
                username = file['OADS_credentials']['username']
                password = file['OADS_credentials']['password']
                selected_collections = file['OADS_credentials']['collections']

                if path_to_data is None and file['Local_file_system']['data_directory'] != "":
                    path_to_data = file['Local_file_system']['data_directory']
            if logger: logger.info(f"Found config file at <{path_to_config}>")
        else:
            raise FileNotFoundError(f"No config file found at <{path_to_config}>. Please make sure you've created one. Run 'python {os.path.basename(__file__)} -h' for help.")
    except FileNotFoundError as e:
        if logger: logger.exception(e)
        raise
    
    if path_to_data is None:
        path_to_data = os.path.dirname(os.path.abspath(__file__))

    try:
        if not os.path.exists(path_to_data):
            raise FileNotFoundError(f"Given data folder does not exist: <{path_to_data}>")
    except FileNotFoundError as e:
        if logger: logger.exception(e)
        raise
    
    if logger:
        console_exclusive_info()
        logger.info(f"Number of pending search requests: {len(planned_requests)}")
    dfs = []
    counter_request = 0
    num_planned_requests = len(planned_requests)
    for request_ixd, search_request in enumerate(planned_requests):
        counter_request = counter_request + 1
        counter_msg, _ = get_counter_message(counter_request, num_planned_requests)

        search_request.collection_identifier_list = [c for c in search_request.collection_identifier_list if c in set(selected_collections)]
        if logger:
            logger.info(f'*{counter_msg} Search request: {search_request.low_detail_summary()}')
            logger.debug(f' {counter_msg} {search_request}')
        collection_identifier_list = search_request.collection_identifier_list
        if len(collection_identifier_list) == 0:
            if logger: logger.warning(f' {counter_msg} No collection was selected. Please make sure that you have added the appropriate collections for this product in the configuration file and that you are allowed to access to them.')
        
        for collection_identifier in collection_identifier_list:
            try:
                url_items = get_url_of_collection_items(collection_identifier, logger=logger)
            except Exception as e:
                if logger: logger.exception(e)
                continue
            dataframe = get_product_list_json(url_items,
                                        product_id_text=None,
                                        sort_by_text=None,
                                        num_results_text=str(int(MAX_NUM_RESULTS_PER_REQUEST)),
                                        start_time_text=search_request.start_time,
                                        end_time_text=search_request.end_time,
                                        poi_text=None,
                                        bbox_text=search_request.bbox,
                                        illum_angle_text=None,
                                        frame_text=search_request.frame_id,
                                        orbit_number_text=search_request.orbit_number,
                                        instrument_text=None,
                                        productType_text=search_request.product_type,
                                        productVersion_text=search_request.product_version,
                                        orbitDirection_text=None,
                                        radius_text=search_request.radius,
                                        lat_text=search_request.lat,
                                        lon_text=search_request.lon,
                                        msg_prefix=f' {counter_msg} ',
                                        logger=logger)
            dataframe = drop_duplicate_files(dataframe, 'id')
            if logger: logger.info(f" {counter_msg} Files found in collection '{collection_identifier}': {len(dataframe)}")
            if len(dataframe) > 0:
                dfs.append(dataframe)
                break

    if len(dfs) > 0:
        dataframe = pd.concat(dfs, ignore_index=True)
    else:
        dataframe = pd.DataFrame()
    
    total_results = len(dataframe)
    if total_results > 0:
        dataframe = drop_duplicate_files(dataframe, 'id')
        dataframe = dataframe.sort_values(by='id')
        if logger:
            console_exclusive_info()
            logger.info(f'List of files found (total number {total_results}):')
        if selected_index is not None:
            try:
                selected_index = dataframe.iloc[[selected_index]].index[0]
            except IndexError:
                raise InvalidInputError(f"The index you selected exceeds the bounds of the found files list (1 - {total_results})")
        for idx, file in enumerate(dataframe['id'].to_numpy()):
            if logger:
                msg = f" [{str(idx+1).rjust(len(str(total_results)))}]  {file}"
                if selected_index is not None and idx == selected_index:
                    msg = f"<[{str(idx+1).rjust(len(str(total_results)))}]> {file} <-- Select file (user input: {download_idx})"
                if total_results > 41:
                    if idx == 20:
                        console_exclusive_info(f' ... {total_results - 40} more files ...')
                    if idx < 20 or total_results - idx <= 20:
                        if not is_debug: console_exclusive_info(msg)
                else:
                    if not is_debug: console_exclusive_info(msg)
                logger.debug(msg)
        if is_found_files_list_to_txt:
            dataframe['id'].to_csv('results.txt', index=False, header=False)
        else:
            logger.info(f"Note: To export this list use the option --export_results")
        if selected_index is not None:
            dataframe = dataframe.iloc[[selected_index]]
        else:
            logger.info(f"Note: To select only one specific file use the option -i/--select_file_at_index")
    else:
        if logger: logger.info(f'No files where found for your request')

    if logger:
        console_exclusive_info()
        log_heading(f"PART 2 - Download products", logger)
        console_exclusive_info()

    download_counter = 0
    unzip_counter = 0
    mean_download_speed = 0
    total_download_size = 0
    if is_download:
        if len(dataframe) == 0:
            if logger: logger.info(f'No products matching the request could be found on the server')
        else:
            download_counter, unzip_counter, mean_download_speed, total_download_size = download(
                dataframe,
                username,
                password,
                path_to_data,
                is_overwrite,
                is_unzip,
                is_delete,
                is_create_subdirs,
                logger=logger
            )
    else:
        if logger: logger.info(f'Skipped since option --no_download was used')
    
    if logger:
        console_exclusive_info()
        log_heading(f"END OF SCRIPT", logger)
        console_exclusive_info()

    time_end_script = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if logger: logger.info(f"Execution time:   {pd.Timestamp(time_end_script) - pd.Timestamp(time_start_script)}")
    size_msg = f"{total_download_size:.2f} MB"
    if total_download_size >= 1024:
        size_msg = f"{total_download_size / 1024:.2f} GB"
    if logger:
        logger.info(f"Files downloaded: {download_counter} ({size_msg} at ~{mean_download_speed:.2f} MB/s)")
        logger.info(f"Files unzipped:   {unzip_counter}")

if __name__ == "__main__":
    args = get_parsed_arguments()

    remove_old_logs(max_num_logs=MAX_NUM_LOGS, max_age_logs=MAX_AGE_LOGS)
    logger = create_logger(args['is_log'], debug=args['is_debug'])

    try:
        main(**args, logger=logger)
    except Exception as e:
        logger.exception(e)
        raise