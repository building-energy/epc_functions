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








