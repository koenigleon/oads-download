# oads-download
`oads_download.py` is a Python script designed to download [EarthCARE](https://earth.esa.int/eogateway/missions/earthcare) data products from ESA's Online Access and Distribution System (OADS). The execution of the script is divided into two parts:
1. Based on user inputs via the command-line search requests are send to the OpenSearch API data catalogue [EO-CAT](https://eocat.esa.int/).
2. The resulting list of products is then downloaded from the OADS servers:
    - https://ec-pdgs-dissemination1.eo.esa.int/
    - https://ec-pdgs-dissemination2.eo.esa.int/

User credentials are not required for the search part but are needed for the download part and must be provided in a separate configuration file (called `config.toml`) where OADS data collections must also be selected depending on your data access authorization. The detailed content of each collection is described [here](https://earth.esa.int/eogateway/faq/esa-earthcare-online-dissemination-service-user-s-guidelines).

The code of this script is developed by Leonard König ([TROPOS](https://www.tropos.de/en/)) based on a Jupyter notebook provided by ESA.
If you have questions please create an issue or contact koenig@tropos.de.

<div style="text-align: left;margin-top: 2em;">
   <a href="http://www.tropos.de/en/" target="_blank"><img src="https://polly.tropos.de/static/images/logos/TROPOS-Logo_ENG_small.png" width="200px" height="71px" alt="http://www.tropos.de/"></a>
</div>

## Table of Contents
- [Setup](https://github.com/koenigleon/oads-download?tab=readme-ov-file#setup)
- [Usage](https://github.com/koenigleon/oads-download?tab=readme-ov-file#usage)
    - [Logging](https://github.com/koenigleon/oads-download?tab=readme-ov-file#logging)
    - [Examples](https://github.com/koenigleon/oads-download?tab=readme-ov-file#examples)
        - [*How can I download specific frames?*](https://github.com/koenigleon/oads-download?tab=readme-ov-file#example-1-how-can-i-download-specific-frames)
        - [*How can I select products within the radius of a ground site?*](https://github.com/koenigleon/oads-download?tab=readme-ov-file#example-2-how-can-i-select-products-within-the-radius-of-a-ground-site)
        - [*How do I obtain data for an entire day?*](https://github.com/koenigleon/oads-download?tab=readme-ov-file#example-3-how-do-i-obtain-data-for-an-entire-day)
        - [*How can I first search for product candidates and then select a single product?*](https://github.com/koenigleon/oads-download?tab=readme-ov-file#example-4-how-can-i-first-search-for-product-candidates-and-then-select-a-single-product)
        - [*How to download orbit ranges?*](https://github.com/koenigleon/oads-download?tab=readme-ov-file#further-examples-how-to-download-orbit-ranges)
- [Tables of product name aliases](https://github.com/koenigleon/oads-download?tab=readme-ov-file#tables-of-product-name-aliases)
  - [Level 1 products](https://github.com/koenigleon/oads-download?tab=readme-ov-file#level-1-products)
  - [Level 2a products](https://github.com/koenigleon/oads-download?tab=readme-ov-file#level-2a-products)
  - [Level 2b products](https://github.com/koenigleon/oads-download?tab=readme-ov-file#level-2b-products)
  - [Auxiliary data](https://github.com/koenigleon/oads-download?tab=readme-ov-file#auxiliary-data)
  - [Orbit data](https://github.com/koenigleon/oads-download?tab=readme-ov-file#orbit-data)

## Setup

- Make sure that you are using a Python environment with the following dependencies:
    - python 3.11+
    - requests
    - numpy
    - pandas
    - beautifulsoup4
    - lxml
- Create a copy of the [example_config.toml](example_config.toml) file and rename it to `config.toml`. It should be located in the same directory as the script `oads_download.py`.
- Enter your OADS credentials as well as the path to your desired data folder.
- Comment out or remove all OADS data collections for which you do not have access authorizations. Otherwise you may not be able to download any data.

## Usage

For detailed explanations on how to use the script and available search criteria run the help command:
```
$ python oads_download.py -h
```

By default, products downloaded with the script are unpacked and stored in the local data folder specified in the `data_directory` of your `config.toml` file. Also, products are organized in a subfolder structure depending on the product level and the acquisition date:
```
data_directory/
├── L1/
│   ├── 2024/
│   │   ├── 11/
│   │   │   ├── 01/
│   │   │   ├── 02/
│   │   │   └── ...
├── L2a/
├── L2b/
├── Meteo_Supporting_Files/
└── Orbit_Data_Files/
```
To prevent this, the `--no_unzip` and `--no_subdirs` options can be used.

### Logging

On execution, log files are created which can be found in the logs folder.
These can be used to trace the execution of the script in more detail than from the console.
Logging can be disabled by using the `--no_log` option.
By default, a maximum of 10 log files are created (older files are automatically deleted).

### Examples

Here are selected examples that illustrate some possible use cases.

#### *Example 1: How can I download specific frames?*
To download the ATL_NOM_1B product for the orbit and frame 02163E you can run the command:
```
$ python oads_download.py ATL_NOM_1B -oaf 2163E
```
If you want to download a product from a specific processor baseline, you can specify its two-letter identifier after a colon or use the `--product_version`/`-pv` option:
```
$ python oads_download.py ATL_NOM_1B:AC -oaf 2163E
```
You can also download different product types with the same command and also use alternative shorthand aliases (see this [table](#table-of-product-name-aliases) below). For example, the following command downloads the products ATL_NOM_1B (baseline AC), CPR_NOM_1B and MSI_RGR_1C for frame 02163E.
```
$ python oads_download.py ANOM:AC MRGR CNOM -oaf 2163E
```
You can also specify only a timestamp within the frame, e.g. if you do not know the orbit and frame identifier in advance (the `--time`/`-t` option allows flexible timestamp string formats, like `202410142355`, `2024-10-14T23:55`, ...):
```
$ python oads_download.py ANOM:AC MRGR CNOM -t 2024-10-14T23:55
```
#### *Example 2: How can I select products within the radius of a ground site?*
```
$ python oads_download.py ATL_EBD_2A --radius 100000 16.878 -24.995 --start_time 2025-01-20T00:00:00 --end_time 2025-01-28T00:00:00
```
With this command the script downloads all ATL_EBD_2A products that are found within a 100 km radius around Mindelo CPV between the 20th and 28th of January 2025.

#### *Example 3: How do I obtain data for an entire day?*
```
$ python oads_download.py AALD -st 20250101 -et 20250102
```
This command downloads all ATL_ALD_2A products for the day of January 1 2025 (125 files) by using the `--start_time`/`-st` and `--end_time`/`-et` options.

#### *Example 4: How can I first search for product candidates and then select a single product?*
```
$ python oads_download.py XORBP -t 20250130 --no_download
```
This lists all AUX_ORBPRE files predicting the orbit on January 30 2025 without downloading them.

The output shows a list of found products with indices:
```
...
List of files found (total number 11):
 [ 1]  ECA_EXAA_AUX_ORBPRE_20250120T000000Z_20250130T000000Z_0001
 [ 2]  ECA_EXAA_AUX_ORBPRE_20250121T000000Z_20250131T000000Z_0001
 [ 3]  ECA_EXAA_AUX_ORBPRE_20250122T000000Z_20250201T000000Z_0001
 [ 4]  ECA_EXAA_AUX_ORBPRE_20250123T000000Z_20250202T000000Z_0001
 [ 5]  ECA_EXAA_AUX_ORBPRE_20250124T000000Z_20250203T000000Z_0001
 [ 6]  ECA_EXAA_AUX_ORBPRE_20250125T000000Z_20250204T000000Z_0001
 [ 7]  ECA_EXAA_AUX_ORBPRE_20250126T000000Z_20250205T000000Z_0001
 [ 8]  ECA_EXAA_AUX_ORBPRE_20250127T000000Z_20250206T000000Z_0001
 [ 9]  ECA_EXAA_AUX_ORBPRE_20250128T000000Z_20250207T000000Z_0001
 [10]  ECA_EXAA_AUX_ORBPRE_20250129T000000Z_20250208T000000Z_0001
 [11]  ECA_EXAA_AUX_ORBPRE_20250130T000000Z_20250209T000000Z_0001
Note: To export this list use the option --export_results
Note: To select only one specific file use the option -i/--select_file_at_index
...
```
To download a single file from this list you can specify its index. To select the last file set the index to -1:
```
$ python oads_download.py XORBP -t 20250130 -i -1
...
List of files found (total number 11):
 [ 1]  ECA_EXAA_AUX_ORBPRE_20250120T000000Z_20250130T000000Z_0001
 [ 2]  ECA_EXAA_AUX_ORBPRE_20250121T000000Z_20250131T000000Z_0001
 [ 3]  ECA_EXAA_AUX_ORBPRE_20250122T000000Z_20250201T000000Z_0001
 [ 4]  ECA_EXAA_AUX_ORBPRE_20250123T000000Z_20250202T000000Z_0001
 [ 5]  ECA_EXAA_AUX_ORBPRE_20250124T000000Z_20250203T000000Z_0001
 [ 6]  ECA_EXAA_AUX_ORBPRE_20250125T000000Z_20250204T000000Z_0001
 [ 7]  ECA_EXAA_AUX_ORBPRE_20250126T000000Z_20250205T000000Z_0001
 [ 8]  ECA_EXAA_AUX_ORBPRE_20250127T000000Z_20250206T000000Z_0001
 [ 9]  ECA_EXAA_AUX_ORBPRE_20250128T000000Z_20250207T000000Z_0001
 [10]  ECA_EXAA_AUX_ORBPRE_20250129T000000Z_20250208T000000Z_0001
<[11]> ECA_EXAA_AUX_ORBPRE_20250130T000000Z_20250209T000000Z_0001 <-- Select file (user input: -1)
Note: To export this list use the option --export_results
...
```

#### *Further examples: How to download orbit ranges?*
Download all D and B frames from orbit 3000 to 3009 (20 files):
```
$ python oads_download.py AALD -f D B -so 3000 -eo 3009
```

Download all frames between 01300D and 01302B (15 files):
```
$ python oads_download.py AALD -soaf 01300D -eoaf 01302B
```

## Tables of product name aliases

### Level 1 products

| Product name | File type  | Shorthand | Notes        |
| ------------ | ---------- | --------- | ------------ |
| A-NOM        | ATL_NOM_1B | ANOM      |              |
| M-NOM        | MSI_NOM_1B | MNOM      |              |
| B-NOM        | BBR_NOM_1B | BNOM      |              |
| C-NOM        | CPR_NOM_1B | CNOM      | JAXA product |
| M-RGR        | MSI_RGR_1C | MRGR      |              |

<details>

<summary>Calibration products</summary>

| Product name | File type  | Shorthand | Notes |
| ------------ | ---------- | --------- | ----- |
| A-DCC        | ATL_DCC_1B | ADCC      |       |
| A-CSC        | ATL_CSC_1B | ACSC      |       |
| A-FSC        | ATL_FSC_1B | AFSC      |       |
| M-BBS        | MSI_BBS_1B | MBBS      |       |
| M-SD1        | MSI_SD1_1B | MSD1      |       |
| M-SD2        | MSI_SD2_1B | MSD2      |       |
| B-SNG        | BBR_SNG_1B | BSNG      |       |
| B-SOL        | BBR_SOL_1B | BSOL      |       |
| B-LIN        | BBR_LIN_1B | BLIN      |       |

</details>

### Level 2a products

| Product name | File type  | Shorthand | Notes        |
| ------------ | ---------- | --------- | ------------ |
| A-FM         | ATL_FM__2A | AFM       |              |
| A-AER        | ATL_AER_2A | AAER      |              |
| A-ICE        | ATL_ICE_2A | AICE      |              |
| A-TC         | ATL_TC__2A | ATC       |              |
| A-EBD        | ATL_EBD_2A | AEBD      |              |
| A-CTH        | ATL_CTH_2A | ACTH      |              |
| A-ALD        | ATL_ALD_2A | AALD      |              |
| M-CM         | MSI_CM__2A | MCM       |              |
| M-COP        | MSI_COP_2A | MCOP      |              |
| M-AOT        | MSI_AOT_2A | MAOT      |              |
| C-FMR        | CPR_FMR_2A | CFMR      |              |
| C-CD         | CPR_CD__2A | CCD       |              |
| C-TC         | CPR_TC__2A | CTC       |              |
| C-CLD        | CPR_CLD_2A | CCLD      |              |
| C-APC        | CPR_APC_2A | CAPC      |              |
| A-CLA        | ATL_CLA_2A | ACLA      | JAXA product |
| M-CLP        | MSI_CLP_2A | MCLP      | JAXA product |
| C-ECO        | CPR_ECO_2A | CECO      | JAXA product |
| C-CLP        | CPR_CLP_2A | CCLP      | JAXA product |

### Level 2b products

| Product name | File type  | Shorthand | Notes        |
| ------------ | ---------- | --------- | ------------ |
| AM-MO        | AM__MO__2B | AMMO      |              |
| AM-CTH       | AM__CTH_2B | AMCTH     |              |
| AM-ACD       | AM__ACD_2B | AMACD     |              |
| AC-TC        | AC__TC__2B | ACTC      |              |
| BM-RAD       | BM__RAD_2B | BMRAD     |              |
| BMA-FLX      | BMA_FLX_2B | BMAFLX    |              |
| ACM-CAP      | ACM_CAP_2B | ACMCAP    |              |
| ACM-COM      | ACM_COM_2B | ACMCOM    |              |
| ACM-RT       | ACM_RT__2B | ACMRT     |              |
| ACMB-3D      | ALL_3D__2B | ALL3D     |              |
| ACMB-DF      | ALL_DF__2B | ALLDF     |              |
| AC-CLP       | AC__CLP_2B | ACCLP     | JAXA product |
| ACM-CLP      | ACM_CLP_2B | ACMCLP    | JAXA product |
| ACMB-RAD     | ALL_RAD_2B | ALLRAD    | JAXA product |

### Auxiliary data

| Product name | File type  | Shorthand | Notes |
| ------------ | ---------- | --------- | ----- |
| X-MET        | AUX_MET_1D | XMET      |       |
| X-JSG        | AUX_JSG_1D | XJSG      |       |

### Orbit data

| Product name | File type  | Shorthand | Notes               |
| ------------ | ---------- | --------- | ------------------- |
| ORBSCT       | MPL_ORBSCT | MPLORBS   | Orbit scenario      |
| ORBPRE       | AUX_ORBPRE | XORBP     | Predicted orbit     |
| ORBRES       | AUX_ORBRES | XORBR     | Reconstructed orbit |
