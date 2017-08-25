# Solr Dump 

Dump a Solr index to file; Read from dumped file to Solr. Inspired by Elasticdump. Not super polished or really intended for mass consumption (more part of Doug Turnbull's set of Solr tools)

## Requirements

  * Python 3.0+
    * pysolr
    
## Usage

Run the scripts directly to get the help to see how to use each one


Script for going from Solr -> line-by-line JSON (each line is a Solr JSON doc)

```
python solrSource.py 
```

Script for going from dumped line-by-line JSON -> Solr

```
python solrDest.py 
```
