# seattle-transit-analysis

### Notes on folderstructure  
Code (pushed):
- contains analysis notebooks and data handling scripts
- contains pickle files, generated from raw data per session

king-county-GTFS-feed (not pushed):
- data from GTFS feed.  raw data.  will be stored seperately.  updates every few weeks.

GIS project (pushed, hopefully):
- ontains the map(s) being made

GIS data (not pushed): 
- other raw data for the map.  stored seperately.  doesn't change often.


### Completed:
- New architecture for calendar processing done (using a .py as the processor, and a .py as a function store)



### Next Steps:
- Clean up the functions for calendar processing (put it all in one)
- create map of Seattle with transit lines and stops on it in QGIS
- Add frequency data to database, and throw it into the data processor .py
- most intuitive way to show changes in stop times