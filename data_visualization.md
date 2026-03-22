# Earthquakes Ingestion Guide

To visualize the data we have we will ingest it into Elasticsearch and use Kibana to explore it. This guide will walk you through the steps to get everything set up and running.

## What is Kibana?

Kibana is a data visualization and exploration tool that works with Elasticsearch. It allows you to create dashboards, visualizations, and perform ad-hoc queries on your data. In this example, we will use Kibana to explore the earthquake and blast datasets after ingesting them into Elasticsearch.

## 1. Project files and their role

The files used to set up the visualiation part are located in two folders:
- `ingest-init`: contains a Dockerfile and a python file that will run a job in charge of sending the data to Elasticsearch. Thus, if you follow the instructions in the [README](README.md) the job has already been running and the data should already be in Elasticsearch.
- `ncedc-earthquakes-dataset`: contains the files related to the dataset and its ingestion instructions. The files are:
    - `ncedc-earthquakes-template.json`: Elasticsearch composable index template (mapping and settings for this project). It defines how the data will be indexed and stored in Elasticsearch. We can think of it as a blueprint for our indices, specifying field types, analyzers, and other settings to optimize search and analysis.
    - `ncedc-earthquakes-dataset/ncedc-earthquakes-pipeline.json`: ingest pipeline that parses CSV rows into structured fields.
    - `ncedc-earthquakes-dataset/earthquakes.txt`: earthquakes dataset.
    - `ncedc-earthquakes-dataset/blasts.txt`: blasts dataset.
    *Note*: The earthquake dataset contains only natural oearthquakes registers, while the blast dataset contains quarries and nuclear tests. Both datasets have the same structure and are ingested into separate indices.
    - `ncedc-earthquakes-dataset/ncedc-earthquakes-dashboards.json`: optional Kibana saved objects for visualization.

## 2. Prerequisites

You must have followed the instructions in the [README](README.md) to set up the kibana part.

## 3. Start Elasticsearch and Kibana

Run (if not done already):

```bash
docker compose up -d
```

Optional health check:

```bash
curl -u elastic:111 http://localhost:9200
```
## 4. Install pipeline and template (local files only)

Just go into Docker Desktop, open the `ingest-init` container and check in the logs to make sure everything went well.

### What does the container do?

As we said before, the container is in charge of loading data to ElasticSearch. If you want to deep dive into the ingestion process, just take a look at [ingest.py](ingest-init/ingest.py).
You will see it is slit in three parts :
1. **wait_for_elasticsearch**:
    This method makes sure ElasticSearch is up and running
2. **install_pipeline_and_template**
    It adds the json that help ElasticSearch understanding the raw data.
3. **ingest_dataset_once**
    It checks if the data is already in the database and adds it if it's not.

## 5. Verify ingestion

You can check the document counts in Elasticsearch with:

```bash
curl -u elastic:111 "http://localhost:9200/ncedc-earthquakes-earthquake/_count?pretty"
curl -u elastic:111 "http://localhost:9200/ncedc-earthquakes-blast/_count?pretty"
```

Expected values are approximately:
- earthquakes: 38095
- blasts: 222

It is also possible to check it directly in Kibana at the [index management page](http://localhost:5601/app/elasticsearch/index_management/indices)

## 7. View data in Kibana

- Open [http://localhost:5601](http://localhost:5601).
- Go to "Discover" ![alt text](img/kibana_menu.png)
- Go on "Dataview" and click on Create a data view ![alt text](img/create-data-view.png)
- Put ***\*earthquake*** in the index name field and click on "Save dataview to Kibana" ![alt text](img/create-data-view2.png)
- ***Warning***: Data is from 1970 to 2020, so you need to change the time range in the top right corner to "Last 50 years" or "All time" ![alt text](img/time-range.png)

### Create a dashboard
- Go to "Dashboard" in the left menu
- Click on "Create new dashboard"
- Click on "Save" and give it a name !
- Click on "Create new visualization" !

At this point you can create any visualization you want. For example, you can create a "Pie chart" classifying earthquakes by magnitude. ![alt text](img/pie_chart_example.png)
- Select the type of chart![alt text](img/pie_chart.png)
- Choose the right parameters for the chart. ![alt text](img/params.png)
- You can then change the values of the parameters to have a clearer chart.

#### Bonus:
You can also create a map visualization to see the distribution of earthquakes in the world. ![alt text](img/map.png)

### Optional: Import pre-built dashboards

You can also import the pre-built dashboards provided in the `ncedc-earthquakes-dataset/ncedc-earthquakes-dashboards.json` file.

To do this:
- Go to "Stack Management" > "Saved Objects"
- Click on "Import" and select [the ndjson dashboard file](ncedc-earthquakes-dataset/ncedc-earthquakes-dashboards.ndjson)

***Warning***: This import comes from a previous version of Kibana, so some visualizations may not work properly. You may need to updata the data source of some visualizations to make them work. To do this, just open the visualization and click on "Edit" to change the data source.
