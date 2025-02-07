# oads-download
`oads_download.py` is a Python script designed to download EarthCARE data products from ESA's Online Access and Distribution System (OADS). The execution of the script is divided into two parts:
1. Based on user inputs via the command-line search requests are send to the OpenSearch API data catalogue [EO-CAT](https://eocat.esa.int/).
2. The resulting list of products is then downloaded from the OADS servers:
    - https://ec-pdgs-dissemination1.eo.esa.int/
    - https://ec-pdgs-dissemination2.eo.esa.int/

User credentials are not required for the search part but are needed for the download part and must be provided in a separate configuration file (called `config.toml`) where OADS data collections must also be selected depending on your data access authorization. The detailed content of each collection is described [here](https://earth.esa.int/eogateway/faq/esa-earthcare-online-dissemination-service-user-s-guidelines).

The code of this script is developed by Leonard König (TROPOS) based on a Jupyter notebook provided by ESA.
If you have questions please create an issue or contact koenig@tropos.de.

## Setup

- Make sure that you are using a Python environment with the following dependencies:
    - python 3.11+
    - requests
    - numpy
    - pandas
    - beautifulsoup4
    - lxml
- Create a copy of the [example_config.toml](example_config.toml) file and rename it to `config.toml`. Enter your credentials as well as the path to your data folder and comment out all OADS data collections for which you do not have access authorizations.

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

Here are selected examples that illustrate some possible use cases.

### *Example 1: How can I download specific frames?*
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
python oads_download.py ANOM:AC MRGR CNOM -t 2024-10-14T23:55
```
### *Example 2: How can I select products within the radius of a ground site?*
```
$ python oads_download.py ATL_EBD_2A --radius 100000 16.878 -24.995 --start_time 2025-01-20T00:00:00 --end_time 2025-01-28T00:00:00
```
With this command the script downloads all ATL_EBD_2A products that are found within a 100 km radius around Mindelo CPV between the 20th and 28th of January 2025.

### *Example 3: How can I first search for product candidates and then select a single product?*
```
$ python oads_download.py XORBP -t 20250130 --no_download
```
This lists all AUX_ORBPRE files predicting the orbit on January 30 2025 without downloading them.

The output shows a list of found products with indices:
```
...
Files found:
 -  1 : ECA_EXAA_AUX_ORBPRE_20250120T000000Z_20250130T000000Z_0001
 -  2 : ECA_EXAA_AUX_ORBPRE_20250121T000000Z_20250131T000000Z_0001
 -  3 : ECA_EXAA_AUX_ORBPRE_20250122T000000Z_20250201T000000Z_0001
 -  4 : ECA_EXAA_AUX_ORBPRE_20250123T000000Z_20250202T000000Z_0001
 -  5 : ECA_EXAA_AUX_ORBPRE_20250124T000000Z_20250203T000000Z_0001
 -  6 : ECA_EXAA_AUX_ORBPRE_20250125T000000Z_20250204T000000Z_0001
 -  7 : ECA_EXAA_AUX_ORBPRE_20250126T000000Z_20250205T000000Z_0001
 -  8 : ECA_EXAA_AUX_ORBPRE_20250127T000000Z_20250206T000000Z_0001
 -  9 : ECA_EXAA_AUX_ORBPRE_20250128T000000Z_20250207T000000Z_0001
 - 10 : ECA_EXAA_AUX_ORBPRE_20250129T000000Z_20250208T000000Z_0001
 - 11 : ECA_EXAA_AUX_ORBPRE_20250130T000000Z_20250209T000000Z_0001
...
```
To download a single file from this list you can specify its index like this:
```
$ python oads_download.py XORBP -t 20250130 -i 4
```
You can also use negative numbers, e.g. if you want to select the last file in the list (`-i -1`).

## Table of product name aliases

| No | Product name | File type | Shorthand |
| --- | --- | --- | --- |
| 1 | A-NOM | ATL_NOM_1B | ANOM |
| 2 | M-NOM | MSI_NOM_1B | MNOM |
| 3 | B-NOM | BBR_NOM_1B | BNOM |
| 4 | B-SNG | BBR_SNG_1B | BSNG |
| 5 | C-NOM | CPR_NOM_1B | CNOM |
| 6 | M-RGR | MSI_RGR_1C | MRGR |
| 7 | X-MET | AUX_MET_1D | XMET |
| 8 | X-JSG | AUX_JSG_1D | XJSG |
| 9 | A-FM | ATL_FM__2A | AFM |
| 10 | A-AER | ATL_AER_2A | AAER |
| 11 | A-ICE | ATL_ICE_2A | AICE |
| 12 | A-TC | ATL_TC__2A | ATC |
| 13 | A-EBD | ATL_EBD_2A | AEBD |
| 14 | A-CTH | ATL_CTH_2A | ACTH |
| 15 | A-ALD | ATL_ALD_2A | AALD |
| 16 | M-CM | MSI_CM__2A | MCM |
| 17 | M-COP | MSI_COP_2A | MCOP |
| 18 | M-AOT | MSI_AOT_2A | MAOT |
| 19 | C-FMR | CPR_FMR_2A | CFMR |
| 20 | C-CD | CPR_CD__2A | CCD |
| 21 | C-TC | CPR_TC__2A | CTC |
| 22 | C-CLD | CPR_CLD_2A | CCLD |
| 23 | C-APC | CPR_APC_2A | CAPC |
| 24 | AM-MO | AM__MO__2B | AMMO |
| 25 | AM-CTH | AM__CTH_2B | AMCTH |
| 26 | AM-ACD | AM__ACD_2B | AMACD |
| 27 | AC-TC | AC__TC__2B | ACTC |
| 28 | BM-RAD | BM__RAD_2B | BMRAD |
| 29 | BMA-FLX | BMA_FLX_2B | BMAFLX |
| 30 | ACM-CAP | ACM_CAP_2B | ACMCAP |
| 31 | ACM-COM | ACM_COM_2B | ACMCOM |
| 32 | ACM-RT | ACM_RT__2B | ACMRT |
| 33 | ACMB-3D | ALL_3D__2B | ALL3D |
| 34 | ACMB-DF | ALL_DF__2B | ALLDF |
| 35 | A-DCC | ATL_DCC_1B | ADCC |
| 36 | A-CSC | ATL_CSC_1B | ACSC |
| 37 | A-FSC | ATL_FSC_1B | AFSC |
| 38 | M-BBS | MSI_BBS_1B | MBBS |
| 39 | M-SD1 | MSI_SD1_1B | MSD1 |
| 40 | M-SD2 | MSI_SD2_1B | MSD2 |
| 41 | B-SOL | BBR_SOL_1B | BSOL |
| 42 | B-LIN | BBR_LIN_1B | BLIN |
| 43 | ORBSCT (orbit scenario) | MPL_ORBSCT | MPLORBS |
| 44 | ORBPRE (predicted orbit) | AUX_ORBPRE | XORBP |
| 45 | ORBRES (reconstructed orbit) | AUX_ORBRES | XORBR |
