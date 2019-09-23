# Springboot
Example of REST APIs with Pagination in Springboot by using Elasticsearch


## Step 1 Download the code


## Step 2 Build the project
```mvn clean install```

## Step 3 Launch the application
```java -jar target\standalone-elasticsearch-0.0.1-SNAPSHOT.jar```

## Step 4  Use Curl or Postman to test the REST apis created. 


## APIs for ElastisearchTemplate usage

### JSON data
```
{
          "id": 1,
          "Storage Type": "ZZ",
          "Source System": "XX",
          "Schema": "WW",
          "Table Name": "AA",
          "Table Description": "History data",
          "Row Count": 76856,
          "Data As Of Date": "2019-05-06",
          "Updated Date": "2019-06-06",
          "Data Domain": ["BB"]
}

```

2. GET call to retrive particular TABLE with id.

http://localhost:8889/md_table_indx/1

3. POST- update field of the document by TABLE id.
http://localhost:8889/md_table_indx/1

4. DELETE-delete field of the document by TABLE id.
http://localhost:8889/md_table_indx/1



