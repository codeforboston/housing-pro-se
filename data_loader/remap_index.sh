#curl -XDELETE penguinwrench.com:9200/housingprose
#curl -XPUT penguinwrench.com:9200/housingprose
curl -XPOST penguinwrench.com:9200/housingprose/eviction/_mapping -d @mapping.json

