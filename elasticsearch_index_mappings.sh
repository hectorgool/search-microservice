
curl -X DELETE localhost:9200/mx



curl -X PUT "http://localhost:9200/mx" -d '
{
    "settings": {
        "index": {
            "analysis": {
                "analyzer": {
                    "autocomplete": {
                        "tokenizer": "whitespace",
                        "filter": [
                            "lowercase",
                            "engram"
                        ]
                    }
                },
                "filter": {
                    "engram": {
                        "type": "edgeNGram",
                        "min_gram": 1,
                        "max_gram": 10
                    }
                }
            }
        }
    },
    "mappings": {
        "postal_code": {
            "_id" : {
                "path" : "id"
            },
            
            "properties": {
                "id": {
                    "type": "string"
                },
                "cp": {
                    "type": "multi_field",
                    "fields": {
                        "cp": {
                            "type" : "float", 
                            "store" : "yes", 
                            "precision_step" : "4"
                        }
                    }
                },
                "colonia": {
                    "type": "multi_field",
                    "fields": {
                        "colonia": {
                            "type": "string",
                            "index": "not_analyzed",
                            "store": "yes"
                        },
                        "autocomplete": {
                            "type": "string",
                            "index_analyzer": "autocomplete",
                            "index": "analyzed",
                            "search_analyzer": "standard"
                        }
                    }
                },                 
                "ciudad": {
                    "type": "multi_field",
                    "fields": {
                        "ciudad": {
                            "type": "string",
                            "index": "not_analyzed",
                            "store": "yes"
                        }
                    }
                },                                
                "delegacion": {
                    "type": "multi_field",
                    "fields": {
                        "delegacion": {
                            "type": "string",
                            "index": "not_analyzed",
                            "store": "yes"
                        }
                    }
                },    
                "location": {
                    "type": "geo_point"
                }
            }
        }
    }
}
'


curl -XPOST "http://localhost:9200/mx/postal_code" -d "
{
    \"id\"    : \"1\",
    \"cp\"         : \"20008\",
    \"colonia\"    : \"Delegación de La Secretaría de Comercio y Fomento Industrial\",
    \"ciudad\"     : \"Aguascalientes\",
    \"delegacion\" : \"Aguascalientes\",
    \"location\": {
        \"lat\": \"22.0074\",
        \"lon\": \"-102.2837\"
    }
}"
