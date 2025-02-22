#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Filename: oads_download.py
Author: Leonard König
Email: koenig@tropos.de
Date: 2025-02-21
Version: 2.6
Description:
    This is a Python script designed to download EarthCARE satellite
    data from the Online Access and Distribution System (OADS) using
    the OpenSearch API data catalogue EO-CAT. You can specify the input
    options through command-line arguments. To see all available options
    and get help, run: 'python oads_download.py -h'. This script is based
    on the `product_search_and_download.ipynb` notebook provided by ESA.
"""

import sys
import os
import re
import time
import argparse
from argparse import RawTextHelpFormatter
import datetime
import tomllib
from zipfile import ZipFile
from urllib.parse import urlparse
from xml.etree import ElementTree
import logging

import requests
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from lxml import html

FRAMES = 'ABCDEFGH'
NUM_FRAMES = len(FRAMES) # 8
CHUNK_SIZE = 256 * 1024 # 256 KB
MAX_NUM_RESULTS_PER_REQUEST = '2000' # needs to be a string

class InvalidInputError(Exception): pass
class BadResponseError(Exception): pass

def validate_request_response(response):
    response.raise_for_status() # raise an for bad responses

format_orbit_and_frame = lambda o, f : str(o).zfill(5) + f.upper()

def get_validated_orbit_number(orbit_number):
    try:
        orbit_number = int(orbit_number)
        if orbit_number < 0 or orbit_number > 99999: raise InvalidInputError()
    except:
        raise InvalidInputError(f"{orbit_number} is not a valid orbit number. Valid orbit numbers are positive integers up to 5 digits.")
    return orbit_number

def get_validated_frame_id(frame_id):
    try:
        frame_id = frame_id.upper()
        if len(frame_id) != 1: raise InvalidInputError()
        if frame_id not in 'ABCDEFGH': raise InvalidInputError()
    except:
        raise InvalidInputError(f"{frame_id} is not a valid frame id. Valid frames are single letters from A to H.")
    return frame_id

def get_validated_orbit_and_frame(orbit_and_frame):
    try:
        orbit_number = int(orbit_and_frame[0:-1])
        frame_id = orbit_and_frame[-1].upper()
    except:
        raise InvalidInputError(f"{orbit_and_frame} is not a valid orbit and frame name. Valid names contain the orbit number followed by the frame id letter (e.g. 3000B or 03000B).")
    return orbit_number, frame_id

def get_counter_message(counter = None, total_count = None):
    max_count_digits = len(str(total_count))
    count_msg = ''
    if counter is not None and total_count is not None:
        count_msg += '[' + str(counter).rjust(max_count_digits) + '/' + str(total_count).rjust(max_count_digits) + ']'
    elif counter is not None:
        count_msg += '[' + str(counter).rjust(max_count_digits) + ']'
    return count_msg, max_count_digits

def unzip_file(filepath,
               delete = False,
               delete_on_error = False,
               counter = None,
               total_count = None):
    count_msg, _ = get_counter_message(counter = counter, total_count = total_count)

    if not os.path.exists(filepath):
        print(f' {count_msg} File not found: {filepath}')
        return False

    # unzip zip file
    print(f' {count_msg} Extracting...', end='\r')
    new_filepath = os.path.join(os.path.dirname(filepath),
                                os.path.basename(filepath).split('.')[0])
    try:
        with ZipFile(filepath, 'r') as zip_file:
            zip_file.extractall(path=new_filepath)
    except:
        if delete_on_error:
            os.remove(filepath)
            print(f' {count_msg} Unzip failed! ZIP-file was deleted.')
        else:
            print(f' {count_msg} Unzip failed! ({filepath})')
        return False

    # delete zip file
    if delete:
        os.remove(filepath)
        print(f' {count_msg} File extracted and ZIP-file deleted. (see {new_filepath})')
    else:
        print(f' {count_msg} File extracted. (see {new_filepath})')
    
    return True

def format_datetime_string(datetime_string):
    timestamp = pd.Timestamp(datetime_string)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize('UTC')
    return timestamp.strftime('%Y-%m-%dT%XZ')

def get_product_type_and_version_from_string(input_string):
    product_name_input = input_string.replace(' ','').replace('-','').replace('_','').lower()
    product_baseline = None
    tmp = product_name_input.split(':')
    if len(tmp) == 2:
        product_baseline = tmp[1].upper()
        if re.fullmatch('[A-Z]{2}', product_baseline) is None:
            raise InvalidInputError(
                f"Product version in '{input_string}' is not valid. \
                Please specify the product version by giving the two-letter processor baseline after the colon (e.g. ':AC')."
            )
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
        # CPR level 1b  #@ JAXA product
        'CPR_NOM_1B',   #@ JAXA product
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
        # MSI level 2a
        'MSI_CM__2A',
        'MSI_COP_2A',
        'MSI_AOT_2A',
        # CPR level 2a
        'CPR_FMR_2A',
        'CPR_CD__2A',
        'CPR_TC__2A',
        'CPR_CLD_2A',
        'CPR_APC_2A',
        # ATLID-MSI level 2b
        'AM__MO__2B',
        'AM__CTH_2B',
        'AM__ACD_2B',
        # ATLID-CPR level 2b
        'AC__TC__2B',
        # BBR-MSI-(ATLID) level 2b
        'BM__RAD_2B',
        'BMA_FLX_2B',
        # ATLID-CPR-MSI level 2b
        'ACM_CAP_2B',
        'ACM_COM_2B',
        'ACM_RT__2B',
        # ATLID-CPR-MSI-BBR
        'ALL_DF__2B',
        'ALL_3D__2B',
        # Orbit data    #@ Orbit files in Auxiliary data collection 
        'MPL_ORBSCT',   #@ orbit scenario file 
        'AUX_ORBPRE',   #@ predicted orbit file
        'AUX_ORBRES',   #@ restituted/reconstructed orbit file
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

    raise InvalidInputError(f'The user input "{input_string}" is either not a valid product name or not supported by this function.\n' + msg + '\n\nor use the respective short hands (additional non letter characters like - or _ are also allowed, e.g. A-NOM):\n' + msg2)

def load_dataframe(response):
  
    # Creating a dataframe with the following columns
    df = pd.DataFrame(
        columns=[
            'dc:identifier', 
            'atom:title', 
            'atom:updated', 
            'atom:link[rel="search"]', 
            'atom:link[rel="enclosure"]', 
            'atom:link[rel="icon"]',
        ]
    )

    # from an OpenSearch query the follwing information is gathered.
    rt = ElementTree.fromstring(response.text)
    for r in rt.findall('{http://www.w3.org/2005/Atom}entry'):
        name = r.find('{http://purl.org/dc/elements/1.1/}identifier').text
        title = r.find('{http://www.w3.org/2005/Atom}title').text
        updated = r.find('{http://www.w3.org/2005/Atom}updated').text
        dcdate = r.find('{http://purl.org/dc/elements/1.1/}date').text

        try:
            href = r.find('{http://www.w3.org/2005/Atom}link[@rel="search"][@type="application/opensearchdescription+xml"]').attrib['href']
        except AttributeError:
            href = ''

        try:
            rel_enclosure = r.find('{http://www.w3.org/2005/Atom}link[@rel="enclosure"]').attrib['href']
        except AttributeError:
            rel_enclosure = ''

        try:
            rel_icon = r.find('{http://www.w3.org/2005/Atom}link[@rel="icon"]').attrib['href']
        except AttributeError:
            rel_icon = ''

        # append a row to the df 
        new_row = {
            'dc:identifier': name,
            'atom:title': title,
            'dc:date': dcdate,
            'atom:updated': updated,
            'atom:link[rel="search"]': href,
            'atom:link[rel="enclosure"]': rel_enclosure,
            'server': urlparse(rel_enclosure).netloc,
            'atom:link[rel="icon"]': rel_icon,
        }

        dfn = pd.DataFrame(new_row, index = [0])
        df = pd.concat([df, dfn], ignore_index=True)

    return df


def get_api_request(template, os_querystring):
    # print("URL template: " + template)

    # Limitation: the OSDD may use a default namespace for OpenSearch instead of using "os".
    # We make a simple correction here allowing to use OpenSearch queryables without namespace in requests.
    # A more generic solution to obtain namespaces from the OSDD and compare them with user supplied namespaces is future work.

    os_namespace = 'os:'

    # perform substitutions in template
    for p in os_querystring:
        result = re.subn(r'\{' + p + r'.*?\}', os_querystring[p] , template)
        n = result[1]
        template = result[0]
        if (n < 1):
            if (':' in p):
                print("ERROR: parameter " + p + " not found in template.")
            else:
                # try with explicit namespace
                result = re.subn(r'\{' + os_namespace + p + r'.*?\}', os_querystring[p] , template)
                n = result[1]
                template = result[0]
                if (n < 1):
                    print("ERROR: parameter " + os_namespace + p + " not found in template.")   

    # remove empty search parameters
    template = re.sub(r'&?[a-zA-Z]*=\{.*?\}', '' , template)

    # remove remaining empty search parameters which did not have an HTTP query parameter attached (e.g. /{time:end}).
    template = re.sub(r'.?\{.*?\}', '' , template)
    template = template.replace('[', '{')
    template = template.replace(']', '}')

    print(" - API request: " + template)

    return (template)

def safe_parse_date(date_str):
    try:
        return pd.to_datetime(date_str, errors="raise")
    except (pd._libs.tslibs.parsing.DateParseError, ValueError):
        return pd.Timestamp.min

def get_product_info_from_path(filepath):
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

    filename_info = dict(
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
        sensing_start_time = safe_parse_date(filename[20:36]),
        processing_start_time = safe_parse_date(filename[37:53]),
        orbit_number = orbit_number,
        frame_id = frame_id,
        orbit_and_frame = orbit_and_frame,
        product_name = product_name,
    )

    return filename_info

def get_product_sub_dirname(product_name):
    if product_name in ['AUX_JSG_1D', 'AUX_MET_1D']:
        sub_dirname = 'Meteo_Supporting_Files'
    elif product_name in ['MPL_ORBSCT', 'AUX_ORBPRE', 'AUX_ORBRES']:
        sub_dirname = 'Orbit_Data_Files'
    elif '0' in product_name:
        sub_dirname = 'L0'
    elif '1' in product_name:
        sub_dirname = 'L1'
    elif '2a' in product_name.lower():
        sub_dirname = 'L2a'
    elif '2b' in product_name.lower():
        sub_dirname = 'L2b'
    return sub_dirname

def ensure_single_zip_extension(filename):
    base_name, ext = os.path.splitext(filename)
    while ext.lower() == '.zip':
        base_name, ext = os.path.splitext(base_name)
    return base_name + '.ZIP'

def get_local_product_dirpath(dirpath_local, filename, create_subdirs=True):
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

def download(dataframe, username, password, download_directory, is_overwrite, is_unzip, is_delete, is_create_subdirs):
    total_count = len(dataframe)
    counter = 1
    download_counter = 0
    unzip_counter = 0
    download_sizes = []
    download_speeds = []
    for server, df_group in dataframe.groupby('server'):
        # 4 Product Download

        ### 4.1. Credentials and Environment Variables
        # Environment variables
        proxies = {}

        # Machine variables
        oads_hostname = server # urlparse(df_group['atom:link[rel="enclosure"]'][0]).netloc
        print(f"Selecting dissemination service: {oads_hostname}")
        eoiam_idp_hostname = "eoiam-idp.eo.esa.int"

        ### 4.2. OADS Login Request
        # requesting access to the OADS machine storing the products
        response = requests.get(f"https://{oads_hostname}/oads/access/login",
                                proxies=proxies)
        validate_request_response(response)

        # extracting the cookies from the response
        cookies = response.cookies
        for r in response.history:
            cookies = requests.cookies.merge_cookies(cookies, r.cookies)
        tree = html.fromstring(response.content)

        # extracting the sessionDataKey from the the response 
        sessionDataKey = tree.findall(".//input[@name = 'sessionDataKey']")[0].attrib["value"]

        ### 4.3. Authentication Login Request
        # defining the payload to send to Authentication platform
        post_data = {
            "tocommonauth": "true",
            "username": username,
            "password": password,
            "sessionDataKey": sessionDataKey,
        }

        # making the request to Authentication platform
        response = requests.post(url=f"https://{eoiam_idp_hostname}/samlsso",
                                 data=post_data,
                                 cookies=cookies,
                                 proxies=proxies)
        validate_request_response(response)

        # parsing the response from Authentication platform
        tree = html.fromstring(response.content)
        responseView = BeautifulSoup(response.text, 'html.parser')
        # print(responseView)

        # extracting the variables needed to redirect from a successful authentication to OADS
        try:
            relayState = tree.findall(".//input[@name='RelayState']")[0].attrib["value"]
            samlResponse = tree.findall(".//input[@name='SAMLResponse']")[0].attrib["value"]
        except:
            raise BadResponseError("OADS did not responde as expected. Check your configuration file for valid a username and password.")
        #saml_redirect_url = f"https://{OADS_HOSTNAME}/oads/Shibboleth.sso/SAML2/POST"
        #saml_redirect_url = tree.findall(f".//form[@action='https://{OADS_HOSTNAME}/oads/Shibboleth.sso/SAML2/POST']")
        saml_redirect_url = tree.findall(".//form[@method='post']")[0].attrib["action"]

        ### 4.4. Redirecting to OADS
        # creating the payload to redirect back to OADS
        post_data = {
            "RelayState": relayState,
            "SAMLResponse": samlResponse,
        }

        # redirecting back to OADS  
        response = requests.post(url=saml_redirect_url,
                                 data=post_data,
                                 proxies=proxies)
        validate_request_response(response)

        cookies2 = response.cookies
        for r in response.history:
            cookies2 = requests.cookies.merge_cookies(cookies2, r.cookies)

        ### 4.5 Downloading Products
        # the amount of retries if a download fails
        max_retries = 3

        # downloading products
        for index, row in df_group.iterrows():
            count_msg, _ = get_counter_message(counter=counter, total_count=total_count)

            success = False

            # extracting the filename from the download link
            file_name = (row['atom:link[rel="enclosure"]']).split("/")[-1]
            product_dirpath = get_local_product_dirpath(download_directory, file_name, create_subdirs=is_create_subdirs)
            # make sure the local download_directory exists (if not create it) 
            if not os.path.exists(product_dirpath): 
                os.makedirs(product_dirpath)
            # XMET files are missing zip file extension so we need to fix them
            file_name = ensure_single_zip_extension(file_name)
            zip_file_path = os.path.join(product_dirpath, file_name)
            file_path = zip_file_path[0:-4]

            print(f"*{count_msg} Starting: {file_name[0:-4]}")

            # defining the download url
            url = row['atom:link[rel="enclosure"]']

            for attempt in range(max_retries):
                if attempt > 0:
                    print(f" {count_msg} Restarting (starting try {attempt + 1} of max. {max_retries}).")
                      
                success = True

                # check existing files
                zip_file_exists = os.path.exists(zip_file_path)
                file_exists = os.path.exists(file_path)

                # plan next steps
                try_download = is_overwrite or (not zip_file_exists and not file_exists)
                try_unzip = is_unzip and (is_overwrite or not file_exists)

                if not try_download:
                    if is_unzip:
                        print(f" {count_msg} Skip file download.")
                    else:
                        print(f" {count_msg} Skip file download. (see {zip_file_path})")
                if not try_unzip:
                    print(f" {count_msg} Skip file unzip. (see {file_path})")
                if not try_download and not try_unzip:
                    counter += 1
                    break

                # delete unnessecary zip files
                if is_delete and file_exists and zip_file_exists:
                    os.remove(zip_file_path)
                    zip_file_exists = False

                # overwrite files
                if zip_file_exists and is_overwrite:
                    os.remove(zip_file_path)
                    zip_file_exists = False
                if file_exists and is_overwrite:
                    os.remove(file_path)
                    file_exists = False

                # download zip file
                if try_download:
                    try:
                        # requesting the product download
                        print(f" {count_msg} Requesting: {url}")
                        response = requests.get(url, 
                                                cookies = cookies2,
                                                proxies = proxies, 
                                                stream = True)
                        validate_request_response(response)
                        
                        with open(zip_file_path, "wb") as f:
                            # print(f" {count_msg} Downloading: {zip_file_path}")
                            total_length = response.headers.get('content-length')
                            if total_length is None:
                                f.write(response.content)
                            else:
                                current_length = 0
                                total_length = int(total_length)
                                start_time = time.time()
                                progress_bar_length = 30
                                for data in response.iter_content(chunk_size=CHUNK_SIZE): 
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
                                    print(f"\r {count_msg} {progress_percentage} {progress_bar} {time_left} - {speed:.2f} MB/s - {size_done:.2f}/{size_total:.2f} MB", end='')
                                time_taken = time.strftime("%H:%M:%S", time.gmtime(int(time.time() - start_time)))
                                print(f"\r {count_msg} Download completed ({time_taken} - {speed:.2f} MB/s - {size_done:.2f}/{size_total:.2f} MB)                   ")
                                download_sizes.append(size_total)
                                download_speeds.append(speed)
                                download_counter += 1
                    except requests.exceptions.RequestException as e:
                        is_error_403_forbidden = False
                        if e.response is not None:  # Ensure response exists
                            is_error_403_forbidden = e.response.status_code == 403
                        if is_error_403_forbidden:
                            attempt = max_retries
                            print(f" {count_msg} DOWNLOAD FAILED: {e}")
                            print(f" {count_msg} Make sure that you only use OADS collections that you are allowed to access in your config.toml (see section 'Setup' in README)!")
                        else:
                            print(f" {count_msg} DOWNLOAD FAILED for attempt {attempt + 1} of {max_retries}: {e}")
                            time.sleep(2)  # wait for 2 seconds before retrying

                    download_success = os.path.exists(zip_file_path)
                    success &= download_success

                # unzip zip file
                if try_unzip:
                    # print(f" {count_msg} Unzipping: {zip_file_path}")
                    success = unzip_file(zip_file_path,
                                         delete=is_delete,
                                         delete_on_error=True,
                                         total_count=total_count,
                                         counter=counter)
                    unzip_success = os.path.exists(file_path)
                    if unzip_success: unzip_counter += 1
                    success &= unzip_success

                if success:
                    counter += 1
                    break

        # logout of authentication platform and OADS
        logout_1 = requests.get(f'https://{oads_hostname}/oads/Shibboleth.sso/Logout',
                                proxies=proxies,
                                stream=True)

        logout_2 = requests.get(f'https://{eoiam_idp_hostname}/Shibboleth.sso/Logout',
                                proxies=proxies,
                                stream=True)
    
    total_download_size = 0 if len(download_sizes) == 0 else np.sum(download_sizes)
    mean_download_speed = 0 if len(download_speeds) == 0 else np.mean(download_speeds)

    return download_counter, unzip_counter, mean_download_speed, total_download_size

def get_product_search_template(collection_identifier):

    print(f' - Trying collection: {collection_identifier}')

    # Get the selected OSDD endpoint from the list.
    url_osdd = 'https://eocat.esa.int/eo-catalogue/opensearch/description.xml'

    # requesting the OSDD fro the catalogue
    response = requests.get(url_osdd)
    validate_request_response(response)

    root = ElementTree.fromstring(response.text)
    # define the root OpenSearch name space
    ns = {'os': 'http://a9.com/-/spec/opensearch/1.1/'}
    collection_url_atom = root.find('os:Url[@rel="collection"][@type="application/atom+xml"]', ns)

    # creating an OpenSearch template for a collection search
    collection_template = collection_url_atom.attrib['template']

    # create a base OpenSearch string
    osquerystring = {}

    # populating the OpenSearch string
    osquerystring['geo:uid'] =  str(collection_identifier)

    # making a request for the API corresponding to the collection chosen from the dropdown menu
    request_url = get_api_request(collection_template, osquerystring)

    response = requests.get(request_url)
    validate_request_response(response)

    ### 3.8. Product API
    root = ElementTree.fromstring(response.text)

    # extract total results
    el = root.find('{http://a9.com/-/spec/opensearch/1.1/}totalResults')
    # print('totalResults: ', el.text)  #@ number of collections

    dataframe = load_dataframe(response)
    
    url_osdd_granules = dataframe.iat[0,3]

    # testing the product API
    response = requests.get(url_osdd_granules, headers={'Accept': 'application/opensearchdescription+xml'})
    validate_request_response(response)

    root = ElementTree.fromstring(response.text)

    # retrieving the template OpenSearch for a product search request 
    granules_url_atom = root.find('{http://a9.com/-/spec/opensearch/1.1/}Url[@rel="results"][@type="application/atom+xml"]')

    # defining the template OpenSearhc request for a product
    template = granules_url_atom.attrib['template']
    
    return template

def get_product_list(product_search_template,
                     product_id_text = None,
                     sort_by_text = None,
                     num_results_text = '1000',
                     start_time_text = None,
                     end_time_text = None,
                     poi_text = None,
                     bbox_text = None,
                     illum_angle_text = None,
                     frame_text = None,
                     orbit_number_text = None,
                     instrument_text = None,
                     productType_text = None,
                     productVersion_text = None,
                     orbitDirection_text = None,
                     radius_text = None,
                     lat_text = None,
                     lon_text = None):

    # define the OpenSearch string
    osquerystring = {}

    # additional search parameters
    if num_results_text:
        osquerystring['count'] = num_results_text
    if end_time_text:
        osquerystring['time:end'] = end_time_text
    if poi_text:
        osquerystring['geo:geometry'] = poi_text
    if bbox_text:
        osquerystring['geo:box'] = bbox_text
    if start_time_text:
        osquerystring['time:start'] = start_time_text
    if product_id_text:
        osquerystring['geo:uid'] = product_id_text
    if sort_by_text:
        osquerystring['sru:sortKeys'] = sort_by_text
    if illum_angle_text:
        osquerystring['eo:illuminationElevationAngle'] = illum_angle_text
    if frame_text:
        osquerystring['eo:frame'] = frame_text
    if orbit_number_text:
        osquerystring['eo:orbitNumber'] = orbit_number_text
    if instrument_text:
        osquerystring['eo:instrument'] = instrument_text
    if productType_text:
        osquerystring['eo:productType'] = productType_text
    if productVersion_text:
        osquerystring['eo:productVersion'] = productVersion_text
    if orbitDirection_text:
        osquerystring['eo:orbitDirection'] = orbitDirection_text
    if radius_text:
        osquerystring['geo:radius'] = radius_text
    if lat_text:
        osquerystring['geo:lat'] = lat_text
    if lon_text:
        osquerystring['geo:lon'] = lon_text

    # make the product request to the catalogue
    request_url = get_api_request(product_search_template, osquerystring)
    request_url = request_url

    response = requests.get(request_url)
    validate_request_response(response)

    root = ElementTree.fromstring(response.text)

    # extract total results
    el = root.find('{http://a9.com/-/spec/opensearch/1.1/}totalResults')

    # extract the results into a dataframe 
    dataframe = load_dataframe(response)

    return dataframe

def drop_duplicate_files(df, filename_column):
    if len(df) == 0:
        return df

    # keep only the latest file (i.e. with latest processing_start_time)
    def extract_info(filename):
        info = get_product_info_from_path(filename)
        return info['product_name'], info['sensing_start_time'], info['processing_start_time']
    df[['product_name', 'sensing_start_time', 'processing_start_time']] = df[filename_column].apply(extract_info).apply(pd.Series)
    df = df.sort_values(by=['product_name', 'sensing_start_time', 'processing_start_time'], ascending=[True, True, False])
    df = df.drop_duplicates(subset=['product_name', 'sensing_start_time'], keep='first')
    df = df.drop(columns=['product_name', 'sensing_start_time', 'processing_start_time'])

    return df.reset_index(drop=True)

def get_frame_range(start_frame_id, end_frame_id):
    start_idx = FRAMES.index(start_frame_id)
    end_idx = FRAMES.index(end_frame_id)
    if end_idx < start_idx:
        end_idx = end_idx + NUM_FRAMES
    frame_id_range = [FRAMES[idx % NUM_FRAMES] for idx in np.arange(start_idx, end_idx + 1)]
    return frame_id_range

def oads_download(
    product_types,
    path_to_data = None,
    timestamps = None,
    frame_ids = None,
    orbit_numbers = None,
    orbit_and_frames = None,
    start_time = None,
    end_time = None,
    radius_search = None,
    bounding_box = None,
    is_download = None,
    is_unzip = None,
    is_delete = None,
    is_overwrite = None,
    is_create_subdirs = None,
    product_version = None,
    path_to_config = None,
    download_idx = None,
    start_orbit_number = None,
    end_orbit_number = None,
    start_orbit_and_frame = None,
    end_orbit_and_frame = None,
):
    if ((start_orbit_and_frame is not None or end_orbit_and_frame is not None) and
        (start_orbit_number is not None or end_orbit_number is not None or orbit_numbers is not None or frame_ids is not None)):
        raise InvalidInputError(f"Options to select a range of obit and frame names (-soaf, -eoaf) can not be used in combination with the options to select only a range of orbits (-o, -so, -eo) or single frames (-f).")
    
    product_types, product_versions = zip(*[get_product_type_and_version_from_string(pn) for pn in product_types])
    product_types, product_versions = list(product_types), list(product_versions)

    # if option '--product_version' is used, all products without explicitly specified baseline in its name (eg. ANOM:AC) will be set to this 
    if product_version is not None:
        product_versions = [product_version if pv == 'latest' else pv for pv in product_versions]
    
    if timestamps is not None:
        timestamps = [format_datetime_string(t) for t in timestamps]

    # handle input of frame ids
    frame_ids_list = None
    if frame_ids is not None:
        tmp_frame_ids = []
        for f in frame_ids:
            f = get_validated_frame_id(f)
            if f not in FRAMES:
                raise InvalidInputError(f"invalid frame ID '{f}'")
            else:
                tmp_frame_ids.append(f)
        frame_ids = tmp_frame_ids
        frame_ids_list = tmp_frame_ids
        is_all_frames = len([f for f in FRAMES if f not in frame_ids_list]) == 0
        if is_all_frames:
            print("!!! --- WARNING ------------------------------------------------------------ !!!")
            print("!!! You used the --frame_id/-f option with all frames (A to H).              !!!")
            print("!!! If you want to download all frame IDs you don't need to use this option. !!!")
            print("!!! ------------------------------------------------------------------------ !!!")
            frame_ids = None
            frame_ids_list = None

    # handle input of orbit numbers
    tmp_orbit_numbers = None
    if (start_orbit_number is None and end_orbit_number is not None):
        raise InvalidInputError(f"End orbit was given ({end_orbit_number}) but start is missing.")
    elif (start_orbit_number is not None and end_orbit_number is None):
        raise InvalidInputError(f"Start orbit was given ({start_orbit_number}) but end is missing.")
    elif start_orbit_number is not None and end_orbit_number is not None:
        if start_orbit_number > end_orbit_number:
            raise InvalidInputError(f"Start orbit ({start_orbit_number}) must be smaller than end orbit ({end_orbit_number}).")
        orbit_number_range = np.arange(start_orbit_number, end_orbit_number + 1)
        tmp_orbit_numbers = orbit_number_range

    if orbit_numbers is not None:
        if tmp_orbit_numbers is not None:
            tmp_orbit_numbers = np.append(tmp_orbit_numbers, orbit_numbers)
        else:
            tmp_orbit_numbers = orbit_numbers
        tmp_orbit_numbers = [int(o) for o in np.sort(tmp_orbit_numbers)]

    # handle input of orbit and frame names
    _start_frame = None
    _end_frame = None
    _orbit_range = None
    orbit_and_frames_list = []
    if (start_orbit_and_frame is None and end_orbit_and_frame is not None):
        raise InvalidInputError(f"End orbit and frame was given ({end_orbit_and_frame}) but start is missing.")
    elif (start_orbit_and_frame is not None and end_orbit_and_frame is None):
        raise InvalidInputError(f"Start orbit and frame was given ({start_orbit_and_frame}) but end is missing.")
    elif start_orbit_and_frame is not None and end_orbit_and_frame is not None:
        _start_orbit_number = int(start_orbit_and_frame[0:-1])
        _start_frame = start_orbit_and_frame[-1].upper()
        _end_orbit_number = int(end_orbit_and_frame[0:-1])
        _end_frame = end_orbit_and_frame[-1].upper()
        _orbit_range = np.arange(_start_orbit_number, _end_orbit_number + 1)
        orbit_and_frame_range = []
        if len(_orbit_range) == 1:
            for f in get_frame_range(_start_frame, _end_frame):
                oaf = format_orbit_and_frame(_orbit_range[0], f)
                orbit_and_frame_range.append(oaf)
        else:
            for f in get_frame_range(_start_frame, 'H'):
                oaf = format_orbit_and_frame(_orbit_range[0], f)
                orbit_and_frame_range.append(oaf)
            for f in get_frame_range('A', _end_frame):
                oaf = format_orbit_and_frame(_orbit_range[-1], f)
                orbit_and_frame_range.append(oaf)
        orbit_and_frames_list = orbit_and_frame_range

        if len(_orbit_range) >= 3:
            tmp_orbit_numbers = _orbit_range[1:-1]
            frame_ids_list = None

    if orbit_and_frames is not None:
        for i, oaf in enumerate(orbit_and_frames):
            _orbit_number, _frame = get_validated_orbit_and_frame(oaf)
            oaf = format_orbit_and_frame(_orbit_number, _frame)
            if _frame not in 'ABCDEFGH':
                raise Exception(f"invalid frame ID '{oaf}'")
            orbit_and_frames[i] = oaf
        orbit_and_frames_list = sorted(set(orbit_and_frames_list + orbit_and_frames))

    if len(orbit_and_frames_list) == 0:
        orbit_and_frames_list = None

    orbit_number_text = None
    if tmp_orbit_numbers is not None:
        orbit_number_text = '[' + ','.join([str(get_validated_orbit_number(o)) for o in tmp_orbit_numbers]) + ']'

    if start_time is not None:
        start_time = format_datetime_string(start_time)
    if end_time is not None:
        end_time = format_datetime_string(end_time)

    if timestamps is not None:
        for t in timestamps:
            if start_time is not None:
                if t < start_time:
                    raise InvalidInputError(f"Timestamp ({t}) must be greater or equal the start time ({start_time}).")
            if end_time is not None:
                if t > end_time:
                    raise InvalidInputError(f"Timestamp ({t}) must be smaller or equal the end time ({end_time}).")

    radius_text = None
    lat_text = None
    lon_text = None
    if radius_search is not None:
        radius_text = str(int(radius_search[0]))
        lat_text = str(float(radius_search[1]))
        lon_text = str(float(radius_search[2]))

    bbox_text= None
    if bounding_box is not None:
        bbox_text = ','.join([str(float(x[1])) for x in bounding_box])


    time_start_script = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print("EARTHCARE OADS DOWNLOAD SCRIPT")
    print()
    print(" "*3, f"START ({time_start_script})")
    print("-"*60)
    print("User inputs:")
    print('\n'.join([
    f' - product_types : {product_types}',
    f' - product_versions : {product_versions}',
    f' - path_to_data : {path_to_data}',
    f' - timestamp : {timestamps}, start_time : {start_time}, end_time : {end_time}',
    f' - frame_id : {frame_ids}',
    f' - orbit_numbers : {orbit_numbers}, start_orbit_number : {start_orbit_number}, end_orbit_number : {end_orbit_number}',
    f' - orbit_and_frame : {orbit_and_frames}, start_orbit_and_frame : {start_orbit_and_frame}, end_orbit_and_frame : {end_orbit_and_frame}',
    f' - radius_search : {radius_search}',
    f' - bounding_box : {bounding_box}',
    f' - is_download : {is_download}, is_unzip : {is_unzip}, is_delete : {is_delete}, is_overwrite : {is_overwrite}',
    f' - product_version : {product_version}',
    f' - path_to_config : {path_to_config}',
    ]))

    planned_requests = []
    df = pd.DataFrame(data=dict(product_types=product_types, product_versions=product_versions))
    for (_product_types, _product_version), group in df.groupby(['product_types', 'product_versions']):
        # ===== Collections ==============================================================
        # EarthCARE Auxiliary Data for Cal/Val Users              : EarthCAREAuxiliary
        # EarthCARE ESA L2 Products                               : EarthCAREL2Validated   # API request does not return search template schema
        # EarthCARE ESA L2 Products for Cal/Val Users             : EarthCAREL2InstChecked
        # EarthCARE ESA L2 Products for the Commissioning Team    : EarthCAREL2Products
        # EarthCARE JAXA L2 Products                              : JAXAL2Validated        # API request does not return search template schema
        # EarthCARE JAXA L2 Products for Cal/Val Users            : JAXAL2InstChecked      # API request does not return search template schema
        # EarthCARE JAXA L2 Products for the Commissioning Team   : JAXAL2Products
        # EarthCARE L0 and L1 Products for the Commissioning Team : EarthCAREL0L1Products
        # EarthCARE L1 Products                                   : EarthCAREL1Validated
        # EarthCARE L1 Products for Cal/Val Users                 : EarthCAREL1InstChecked
        # EarthCARE Orbit Data                                    : EarthCAREOrbitData     # API request does not return search template schema
        # ================================================================================
        collection_identifier_list = [
            'EarthCAREL0L1Products',
            'EarthCAREL1InstChecked',
            'EarthCAREL1Validated',
            'EarthCAREL2Products',
            'EarthCAREL2InstChecked',
            'JAXAL2Products',
            'EarthCAREAuxiliary',
        ]
        if _product_types.split('_')[-1] in ['1B', '1C']:
            collection_identifier_list = [
                'EarthCAREL0L1Products',
                'EarthCAREL1InstChecked',
                'EarthCAREL1Validated',
            ]
        elif _product_types.split('_')[-1] in ['2A', '2B']:
            collection_identifier_list = [
                'EarthCAREL2Products',
                'EarthCAREL2InstChecked',
                'JAXAL2Products',
            ]
        elif _product_types.split('_')[-1] in ['1D']:
            collection_identifier_list = ['EarthCAREL0L1Products']
        elif _product_types.split('_')[-1] in ['ORBSCT', 'ORBPRE', 'ORBRES']:
            collection_identifier_list = ['EarthCAREAuxiliary']

        _product_types = np.array([_product_types])
        if _product_version == 'latest':
            _product_version = None

        product_type_text = '[' + ','.join(_product_types) + ']'
        type_and_version = dict(product_type=product_type_text, product_version=_product_version)
        radius_and_bbox = dict(radius=radius_text, lat=lat_text, lon=lon_text, bbox=bbox_text)
        if timestamps is not None:
            for t in timestamps:
                new_request = dict(
                    **type_and_version,
                    collection_identifier_list=collection_identifier_list,
                    start_time=t,
                    end_time=t,
                    orbit_number=None,
                    frame_id=None,
                    radius=None,
                    lat=None,
                    lon=None,
                    bbox=None
                )
                planned_requests.append(new_request)

        if orbit_and_frames_list is not None:
            if _orbit_range is not None and orbit_number_text is not None:
                new_request = dict(
                    **type_and_version,
                    collection_identifier_list=collection_identifier_list,
                    start_time=start_time,
                    end_time=end_time,
                    orbit_number=orbit_number_text,
                    frame_id=None,
                    **radius_and_bbox
                )
                planned_requests.append(new_request)
            for oaf in orbit_and_frames_list:
                _frame = oaf[-1]
                _orbit = int(oaf[0:-1]) 
                new_request = dict(
                    **type_and_version,
                    collection_identifier_list=collection_identifier_list,
                    start_time=start_time,
                    end_time=end_time,
                    orbit_number=str(_orbit),
                    frame_id=_frame,
                    **radius_and_bbox
                )
                planned_requests.append(new_request)

        if frame_ids_list is not None:
            for f in frame_ids_list:
                new_request = dict(
                    **type_and_version,
                    collection_identifier_list=collection_identifier_list,
                    start_time=start_time,
                    end_time=end_time,
                    orbit_number=orbit_number_text,
                    frame_id=f,
                    **radius_and_bbox
                )
                planned_requests.append(new_request)
        if len(planned_requests) == 0:
            new_request = dict(
                **type_and_version,
                collection_identifier_list=collection_identifier_list,
                start_time=start_time,
                end_time=end_time,
                orbit_number=orbit_number_text,
                frame_id=None,
                **radius_and_bbox
            )
            planned_requests.append(new_request)

    # read credentials
    if path_to_config is None:
        path_to_script = os.path.abspath(__file__)
        path_to_script_dir = os.path.dirname(path_to_script)
        path_to_config = os.path.join(path_to_script_dir, 'config.toml')
        print(f"Setting path_to_config to: {path_to_config}")
    username = ""
    password = ""

    time_search_products = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print(" "*3, f"SEARCH PRODUCTS ({time_search_products})")
    print("-"*60)
    print('Reading credentials ...')
    if os.path.exists(path_to_config):
        with open(path_to_config, 'rb') as f:
            file = tomllib.load(f)
            username = file['OADS_credentials']['username']
            password = file['OADS_credentials']['password']
            selected_collections = file['OADS_credentials']['collections']

            if path_to_data is None and file['Local_file_system']['data_directory'] != "":
                path_to_data = file['Local_file_system']['data_directory']
    else:
        raise FileNotFoundError(f"No config file found at {path_to_config}. Please make sure you've created one. Run 'python {os.path.basename(__file__)} -h' for help.")

    if path_to_data is None:
        path_to_data = os.path.dirname(os.path.abspath(__file__))

    if not os.path.exists(path_to_data):
        raise FileNotFoundError(f"Given data folder does not exist: '{path_to_data}'")
    
    print(f"Number of pending search requests: {len(planned_requests)}")
    dfs = []
    for request_ixd, pr in enumerate(planned_requests):
        pr['collection_identifier_list'] = [c for c in pr['collection_identifier_list'] if c in set(selected_collections)]
        filtered_pr = {k: v for k, v in pr.items() if v is not None}
        print(f'Start search request {request_ixd+1} of {len(planned_requests)}: {filtered_pr}')
        collection_identifier_list = pr['collection_identifier_list']
        if len(collection_identifier_list) == 0:
            print(f' - WARNING! No collection was selected. Please make sure that you have added the appropriate collections for this product in the configuration file and that you are allowed to access to them.')
        for collection_identifier in collection_identifier_list:
            template = get_product_search_template(collection_identifier)
            dataframe = get_product_list(template,
                                        product_id_text = None,
                                        sort_by_text = None,
                                        num_results_text = MAX_NUM_RESULTS_PER_REQUEST,
                                        start_time_text = pr['start_time'],
                                        end_time_text = pr['end_time'],
                                        poi_text = None,
                                        bbox_text = pr['bbox'],
                                        illum_angle_text = None,
                                        frame_text = pr['frame_id'],
                                        orbit_number_text = pr['orbit_number'],
                                        instrument_text = None,
                                        productType_text = pr['product_type'],
                                        productVersion_text = pr['product_version'],
                                        orbitDirection_text = None,
                                        radius_text = pr['radius'],
                                        lat_text = pr['lat'],
                                        lon_text = pr['lon'])
            dataframe = drop_duplicate_files(dataframe, 'dc:identifier')
            print(f' - Request results: {len(dataframe)}')
            if len(dataframe) > 0:
                dfs.append(dataframe)
                break

    if len(dfs) > 0:
        dataframe = pd.concat(dfs, ignore_index=True)
    else:
        dataframe = pd.DataFrame()
    
    total_results = len(dataframe)
    if total_results > 0:
        dataframe = drop_duplicate_files(dataframe, 'dc:identifier')
        dataframe = dataframe.sort_values(by='dc:identifier')
        print('Files found:')
        for idx, file in enumerate(dataframe['dc:identifier'].to_numpy()):
            print(f" - {str(idx+1).rjust(len(str(total_results)))} : {file}")
        print(f'Total: {total_results}')
        if download_idx is not None:
            try:
                dataframe = dataframe.iloc[[download_idx]]
            except IndexError:
                raise InvalidInputError(f"The index you selected exceeds the bounds of the found files list (1 - {total_results})")
            print(f'Selecting index {download_idx}:')
            print(f" - {dataframe['dc:identifier'].to_numpy()[0]}")
    else:
        print(f'No files where found for your request')

    time_download_products = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print(" "*3, f"DOWNLOAD PRODUCTS ({time_download_products})")
    print("-"*60)
    download_counter = 0
    unzip_counter = 0
    mean_download_speed = 0
    total_download_size = 0
    if is_download:
        if len(dataframe) == 0:
            print(f'No products matching the request could be found on the server')
        else:
            download_counter, unzip_counter, mean_download_speed, total_download_size = download(
                dataframe,
                username,
                password,
                path_to_data,
                is_overwrite,
                is_unzip,
                is_delete,
                is_create_subdirs
            )
    else:
        print(f'Skipped since option --no_download was used')
    
    time_end_script = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print()
    print(" "*3, f"END ({time_end_script})")
    print("-"*60)
    print("Summary:")
    print(f" - Execution time : {pd.Timestamp(time_end_script) - pd.Timestamp(time_start_script)}")
    size_msg = f"{total_download_size:.2f} MB"
    if total_download_size >= 1024:
        size_msg = f"{total_download_size / 1024:.2f} GB"
    print(f" - Files downloaded : {download_counter} ({size_msg} at ~{mean_download_speed:.2f} MB/s)")
    print(f" - Files unzipped : {unzip_counter}")

if __name__ == "__main__":

    description = """This Python script is designed to download EarthCARE data products
from ESA's Online Access and Distribution System (OADS).
A configuration file containing your OADS credentials is required to use it.
If you don't have one yet, simply create a file called 'config.toml'
in the script's folder and enter the following content:
───config.toml───────────────────────────────────────────────────────────────────────────────────
[Local_file_system]
data_directory = ''
[OADS_credentials]
username = 'your_username'
password = \"\"\"your_password\"\"\" # use triple quotation marks to allow for special characters
# You need to comment out or remove all collections to which you do not have access rights to
collections = [
    'EarthCAREAuxiliary',     # EarthCARE Auxiliary Data for Cal/Val Users
    'EarthCAREL2Validated',   # EarthCARE ESA L2 Products
    'EarthCAREL2InstChecked', # EarthCARE ESA L2 Products for Cal/Val Users
    'EarthCAREL2Products',    # EarthCARE ESA L2 Products for the Commissioning Team
    'JAXAL2Validated',        # EarthCARE JAXA L2 Products
    'JAXAL2InstChecked',      # EarthCARE JAXA L2 Products for Cal/Val Users
    'JAXAL2Products',         # EarthCARE JAXA L2 Products for the Commissioning Team
    'EarthCAREL0L1Products',  # EarthCARE L0 and L1 Products for the Commissioning Team
    'EarthCAREL1Validated',   # EarthCARE L1 Products
    'EarthCAREL1InstChecked', # EarthCARE L1 Products for Cal/Val Users
    'EarthCAREOrbitData',     # EarthCARE Orbit Data
]
─────────────────────────────────────────────────────────────────────────────────────────────────
Recommodations: If you want to check the search results first, use the '--no_download' option.
                This way no data is downloaded yet. Also if you are looking for only one specific
                file you may also use the '--select_file_at_index' option to download only one
                select file from the found files list (see option descriptions below)."""

    parser = argparse.ArgumentParser(
                    prog='oads_download',
                    description=description,
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
                        help="Enables logging for requests.")
    parser.add_argument("-i", "--select_file_at_index",
                        type = int,
                        default = None,
                        help = "Select only one product from the found products list by index for download. You may provide a negative index to start from the last entry (e.g. -1 downloads the last file listed).")
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    selected_index = args.select_file_at_index
    if selected_index is not None:
        if selected_index >= 1:
            selected_index = selected_index - 1
        elif selected_index == 0:
            raise InvalidInputError("The indices in the found files list start at 1.")

    oads_download(
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
    )