# data_loader.py

For loading the eviction dataset into Elasticsearch.

* Start by setting up an ELK (Elasticsearch/Logstash/Kibana) stack via docker. See this repository for everything you need: https://github.com/deviantony/docker-elk
* Update the configuration in loadData.py with the hostname and port of your Elasticsearch instance
* Set up a Python virtual environment
* Run loadData.py
