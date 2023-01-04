# General

This project is the second project of 1st semester (2022-2023) course **Full Stack Web Development** of the postgraduate program **_Informatics and Telematics_** of **Harokopio University**.

It implements an ETL pipeline using **Flask**, **MongoDB**, **Neo4j**, **MySQL** and **Kafka**.

# Components

Seperate pipeline components are Dockerized using **Docker** and **Docker Compose** and are the following:

1. A **MongoDB** component were we will have a database _products_ with a number of collections as product categories each containing a number of products as documents.
2. A **Mongo Express UI** component to browse the MongoDB database.
3. A **Neo4j** component were we will have _user_ nodes with their _name_ and a _products_ list for each one, with the product ids of some of the products of the _products_ MongoDB database. The users will be connected by _friendship_ relationships.
4. A **Kafka** component were _collections_ of products from MongoDB will be stored in corresponding _collection_ topics and _users_ connected with friendship relationships in a _users_ topic.
5. A **Zookeeper** component on which Kafka depends.
6. A **Kafka UI** component to visualize the Kafka state each moment.
7. A **MariaDB** component were data read from Kafka will be stored
8. A **phpMyAdmin** component to browse the data stored in MariaDB database
9. A **Flask** component which, through appropriate REST endpoints, will act as
   - A Kafka _producer_ reading data from MongoDB and Neo4j databases and send them to Kafka
   - A Kafka _consumer_ reading data from kafka and send them to MariaDB database

The mechanism of the pipeline is illustrated below (figure by the teacher John Violos)

<p align="center"><img src="./resources/ETL-Pipeline.jpg" alt="ETL-Pipeline" width="750"/></p>

# Build and run the application

- Docker needs to be installed
- Make an `.env` file in root level (where the `docker-compose.yaml` file is) with vars:
  - `DB_USERNAME=admin`
  - `DB_PASSWORD=password`
  - `NEO4J_USERNAME=neo4j`
  - `NEO4J_PASSWORD=password`
- Launch a terminal in root level and RUN `docker-compose up`.
- When all services are up and running,
  - Launch a new terminal and RUN `docker exec -it web-container bash` to run commands on the **Flask** container named `web-container`.
  - Once you are in the container, RUN `cd src && python -m flaskapp.initialize --categories-num=8` (if `--categories-num=8` is omitted, will default to 8). This will create the _products_ database and 8 collections of products from the `products.json` file, as well as dummy users for Neo4j as described above.
- **Mongo Express UI** will be listening on `http://localhost:8081`
- **Neo4j** will be listening on `http://localhost:7474/browser` (credentials as in `.env` file `NEO4j_...`)
- **Kafka UI** will be listening on `http://localhost:8080`
- **phpMyAdmin** will be listening on `http://localhost:8082` (credentials as in `.env` file `DB_...`)
- **Flask** app will be available on `http://localhost:5000`
  - Through `http://localhost:5000/collection/<collection-name>` the contents of the MongoDB products collection _collection-name_ will be sent to Kafka
  - Through `http://localhost:5000/user/<user-id>` the Neo4j user with id _user-id_ along with the users connected with it will be sent to Kafka
  - Through `http://localhost:5000/collection/<collection-name>/user/<user-id>` the contents of the latest offset of kafka topic with name _collection-name_ will be read along with the user data of the kafka topic with name _users_ and of the latest offset where the user with this id is found and the matched infomation will populate the corresponding tables on MariaDB database (tables `users`, `categories`, `products`, `transactions`)