# About #

Stratio Connector Commons  is a helper library for stratio connector.

# Main features #

Easy to deal with API
---------------------

Abstract classes providing common works. It manages the existing connections and its jobs. For instance, when a query is executed, a new job is started. This way, if the user closes the connector through close(), the connector wait to finish his pending jobs. Furthermore, this module offer a more simple API to develop connectors. 

CommonsConnector: implements IConnector crossdata interface. 
CommonsMetadataEngine: implements IMetadataEngine interface.
CommonsStorageEngine: implements IMetadataEngine interface.
CommonsQueryEngine: implements IMetadataEngine interface.
SingleProjectQueryEngine: extends CommonsQueryEngine. It should be used when the connector does not support joins, so there would be only one project.


In order to use this modules, it is necessary to create an implementation of the following classes which are included in this project:

Connection
ConnectionHandler

ConnectionHandler: Este es el manejador de las conexiones del connector. Se pueden recuperar, desconectar y almacenar las conexiones. La conexión a manejar debe cumplir la interfaz Connection

Usage examples can be found in connectors using this features: 

[MongoDB](https://github.com/Stratio/stratio-connector-mongodb
[Streaming](https://github.com/Stratio/stratio-connector-streaming
[HDFS](https://github.com/Stratio/stratio-connector-hdfs
[Elasticsearch](https://github.com/Stratio/stratio-connector-elasticsearch
[Deep-Spark](https://github.com/Stratio/stratio-connector-deep
[Aerospike](https://github.com/Stratio/stratio-connector-aerospike

Functional test
---------------

A set of functional test with most of the Crossdata features. To run these generic test is needed: 

- Extend the functional test. It is possible either override or ignore the test which features are not implemented. 

- Implements a ConnectorHelper. Here, the cluster configuration and the database specific properties are defined. Therefore, you could use different implementations to probe the test with the wished configuration.

The list of functional test implemented is detailed [here](_doc/FunctionalTests.md).

Utilities
---------

Helper classes for simplifying testing:

LogicalWorkflowCreator: to create LogicalWorkflow
CatalogMetadataBuilder: to create CatalogMetadata
TableMetadataBuilder: to create TableMetadata
IndexMetadataBuilder: to create IndexMetadata


Commons processing when working with Crossdata structures are included:

ManifestUtil: Parses the Connector manifest to extract either the connector or the datastore name. It should be used to allow configure different connector names. 
SelectorHelper: Allows cast Crossdata Selectors to retrieve the proper value.
ProjectParsed: Parses and validates the Crossdata logical steps reading the workflow from a Project. 
ColumnTypeHelper: Allows cast an object based on the columntType.
ConnectorParser: Parses hosts and ports with different formats.
Se encarga de realizar transformaciones de un string que representa host o puertos a un String[]

# License #

Licensed to STRATIO (C) under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  The STRATIO (C) licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.





