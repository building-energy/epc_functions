# epc_functions
Python package for working with the Energy Performance Certificates dataset from Open Data Communities

## Overview

The Energy Performance Certificates dataset is published by the UK Government here: https://epc.opendatacommunities.org/

This Python package contains a series of Python functions which can be used to:
- extract the CSV files which make up the EPC dataset.
- Import the data into a SQLite database.
- Access the data in the database, for data analysis and visualisation.

A description of the CSV files in the EPC dataset, along with instructions for extracting and importing in SQLite, has been created using the format of a [CSVW metadata Table Group object](https://www.w3.org/TR/2015/REC-tabular-metadata-20151217/#table-groups) (saved as three JSON files), which are available here: https://github.com/building-energy/epc_functions

## Installation

`pip install git+https://github.com/building-energy/epc_functions`

The python package [`csvw_functions_extra`](https://github.com/stevenkfirth/csvw_functions_extra) will also need to be installed.

## Quick Start

Before running the code below, the ZIP file of the dataset must be downloaded from the [Open Data Communities website](https://epc.opendatacommunities.org/) and placed in a relative folder `_data`.

```python
import epc_functions

# extracts EPC data from a single local authority, for the year 2021, and import into database.
epc_functions.extract_and_import_data(
        csv_zip_extract_paths = [
                'domestic-E07000044-South-Hams/certificates.csv',  # see the function `get_csv_zip_extract_paths_in_zip` below.
                'domestic-E07000044-South-Hams/recommendations.csv'
                ],
        inspection_date_start='2021-01-01',
        inspection_date_end='2021-12-31'
        )

# get the distribution of property types in the data.
result=epc_functions.get_domestic_certificates_count(
                group_by = 'PROPERTY_TYPE'
                )
print(result)
```
```python
[
        {'PROPERTY_TYPE': 'Bungalow', 'COUNT': 459}, 
        {'PROPERTY_TYPE': 'Flat', 'COUNT': 368}, 
        {'PROPERTY_TYPE': 'House', 'COUNT': 1830}, 
        {'PROPERTY_TYPE': 'Maisonette', 'COUNT': 71}
]
```


## API

### get_csv_zip_extract_paths_in_zip

Description: Returns a list of CSV extract paths in the EPC ZIP file.

```python
epc_functions.get_csv_zip_extract_paths_in_zip(
        zip_filename = 'all-domestic-certificates.zip',
        data_folder = '_data'
        )
```

Arguments:
- **zip_filename** *(str)*: The file name of the ZIP file downloaded from the Open Data Communities website and saved in the data folder.
- **data_folder** *(str)*: The path of the folder where the ZIP file is saved.

Returns *(list)*: A list of the CSV extract paths in the ZIP file.


### extract_and_import_data

Description: Extracts all the EPC data and imports all data into a SQLite database.

```python
epc_functions.extract_and_import_data(
        zip_filename = 'all-domestic-certificates.zip',
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        csv_zip_extract_paths = None,
        set_certificates = True,
        set_recommendations = True,
        inspection_date_start=None,
        inspection_date_end=None,
        verbose=False
        )
```

Arguments:
- **zip_filename** *(str)*: The file name of the ZIP file downloaded from the Open Data Communities website and saved in the data folder.
- **data_folder** *(str)*: The path of the folder where the ZIP file is saved.
- **database_name** *(str)*: The file name of the database.
- **csv_zip_extract_paths** *(str or list)*: The ZIP extract path(s) of CSV files to be extracted and imported.
- **set_certificates** *(bool)*: If True, then certificates.csv files are included; If False, they are excluded.
- **set_recommendations** *(bool)*: If True, then recommendations.csv files are included; If False, they are excluded.
- **inspection_date_start** *(str)*: The earliest INSPECTION_DATE to filter on.
- **inspection_date_end** *(str)*: The latest INSPECTION_DATE to filter on.
- **verbose** *(bool)*: If True, then this function prints intermediate variables and other useful information.

Returns: None


### get_epc_table_names_in_database

Description: Returns the table names of all EPC tables in the SQLite database.

```python
epc_functions.get_epc_table_names_in_database(
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        )
```

Arguments:
- **data_folder** *(str)*: The filepath of a local folder where the SQLite database is stored.
- **database_name** *(str)*: The name of the SQLite database, relative to the data_folder.

Returns *(list)*: A list of table names.


### get_domestic_certificates

```python
get_domestic_certificates(
        filter_by = None,  
        fields = None,  
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        verbose = False
        )
```


### get_domestic_certificates_count

```python
get_domestic_certificates_count(
        filter_by = None,
        group_by = None,
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        verbose = False
        )
```


### get_domestic_certificates_field_names

```python
get_domestic_certificates_field_names(
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        verbose = False
        )
```

### get_domestic_recommendations

```python
get_domestic_recommendations(
        filter_by = None,  
        fields = None, 
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        verbose = False
        )
```


### get_domestic_recommendations_count

```python
get_domestic_recommendations_count(
        filter_by = None,
        group_by = None,
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        verbose = False
        )
```


### get_domestic_recommendations_field_names


```python
get_domestic_recommendations_field_names(
        data_folder = '_data',
        database_name = 'epc_data.sqlite',
        )
```

