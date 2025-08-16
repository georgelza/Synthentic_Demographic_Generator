# Getting the basic blocks.

Ok, before we can do anything we need the base docker images downloaded.

At this stage the below will be downloaded by running `make pull`.

- Confluent Kafka stack
- MongoDB-Atlas
- Postgres 12
- Redis
- Neo4j

You will notice I also included *Neo4j* as a service in `docker-compose.yml` file. 

This is as this is actually part of a bigger blog series...

This data generated using this appliaction is to be imported/sinked into a Neo4J database as nodes and for which edges, aka links are then created. 

This will be done in the next blog (itself following on a previous [Fraud Analytics using a different approach, GraphDB data platform (part 1)](https://medium.com/@georgelza/fraud-analytics-using-a-different-approach-graphdb-data-platform-part-1-807c68d03bff)), where upon I will update this blog with the link to that...

Note there was a [Fraud Analytics using a different approach, GraphDB data platform (part 2](https://medium.com/@georgelza/fraud-analytics-using-a-different-approach-graphdb-data-platform-part-2-b7f69d872192) to this series.


The above blog I mention will be part 3 or is that maybe 4.., we'll see.


## Management/Data Explore Tooling

- Mongo Compass  => https://www.mongodb.com/try/download/compass

- PG Admin       => https://www.pgadmin.org

- Redis Insight  => https://redis.io/insight/
  
- For Kafka you will use the Web based Confluent Control Center available on localhost:9021

- For Neo4J you will use the Web based browser available on localhost:7474