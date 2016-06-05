if (( $# < 1 )); then
    echo "Please pass the version of the index"
    exit 1
fi

NEW_INDEX="housingprose_$1"

#curl -XDELETE penguinwrench.com:9200/housingprose
curl -XPUT penguinwrench.com:9200/$NEW_INDEX
echo ''
curl -XPOST penguinwrench.com:9200/$NEW_INDEX/eviction/_mapping -d @mapping.json
echo ''

# Swap the alias over to the new index
curl -XDELETE penguinwrench.com:9200/_alias/housingprose
echo ''

curl -XPOST penguinwrench.com:9200/_aliases -d " {\"actions\" : [{ \"add\" : {\"index\":\"$NEW_INDEX\",\"alias\": \"housingprose\"}}]}"
echo ''


