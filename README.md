# Elastic search

## Installation
1. Clone this repository : 
2. Open a terminal in the folder.
3. Launch `docker compose up -d`
    - At this point your elastic search server should already be up and running.
    - Verify this by connection to : [http://localhost:9200](http://localhost:9200) (Use the credentials in the Docker compose)
4. When you connect to the website, you'll see this:
![alt text](img/image.png)
5. To connect, use:
    - **elastic** for the username
    - The value of *ELASTIC_PASSWORD* found in the docker-compose.yml file
6. If the connection is right, you should see a message similar to this : ![alt text](img/elastic_login.png)

If the installation went well, you should be able to access Kibana at [http://localhost:5601](http://localhost:5601) and see this:
![alt text](img/kibana_login.png)
Use the elastic username and password (used in step I.5)
When everything is set, you will see this page:
![alt text](img/kibana_home.png)

Kibana is a powerful tool to visualize and analyze data stored in elastic search. You can create dashboards, visualizations, and perform complex queries to gain insights from your data. You will use it after you finish the [Elasticsearch Basics section](Elasticsearch_Basics.ipynb) to visualize the data you indexed in elastic search.

## Assignment
1. Follow the [Elasticsearch Basics section](Elasticsearch_Basics.ipynb)
2. Use Kibana to visualize the data (follow [data visualization](data_visualization.md) tutorial)