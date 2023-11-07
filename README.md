# epc_functions
Python package for working with the Energy Performance Certificates dataset from Open Data Communities




## API

### get_csv_file_names_in_zip

Description: Returns a list of CSV files in the EPC ZIP file.

```python
epc_functions.get_csv_file_names_in_zip(
        zip_filename = 'all-domestic-certificates.zip',
        data_folder = '_data'
        )
```

Arguments:
- **zip_filename** *(str)*: The file name of the ZIP file downloaded from the Open Data Communities website and saved in the data folder.
- **data_folder** *(str)*: The path of the folder where the ZIP file is saved.

Returns *(list)*: A list of the CSV file names in the ZIP file.


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



