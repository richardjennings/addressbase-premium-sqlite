# AddressBase Premium SQLite

Populate a SQLite Database with data from Ordnance Surveys AddressBase Premium 
product.

## Status

Unverified - pre-alpha

-[x] Delivery Point Address Table
-[ ] Other Data Types

## Run

```
make build

./abs <path to folder full of zipped csvs>
```

## Performance

On a Macbook Air, 10962 zip files processed in 332s with 30785258 delivery 
point address rows.

- Read directly from Zip files - no need to expand
- No FSync guarantee - if the script does not finish - the DB is in an undefined 
state.
- Exclusive Lock - no reading by other processes while the script is running.
- Cache - allow up to 10Mi 
- MMap - allow up to 5GB space to be memory mapped
- No indexes - add any required indexes after bulk insert
- No Foreign keys - add them after bulk inserts


