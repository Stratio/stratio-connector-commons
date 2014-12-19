# About #

Stratio Connector Commons provides the classes shared by Stratio connectors.

# Main features #

User friendly API
---------------------

This API consists on a set of abstract classes that provides common work related to Stratio connectors. It manages the existing connections and relevant  jobs. For instance, when a query is executed, a new job is started. Thus, if the user closes the connector using close(), the connector waits until the pending jobs are done to finish. Furthermore, this module offers a simpler API to develop connectors. 

 * CommonsConnector: implements IConnector Crossdata interface. 
 * CommonsMetadataEngine: implements IMetadataEngine interface.
 * CommonsStorageEngine: implements IMetadataEngine interface.
 * CommonsQueryEngine: implements IMetadataEngine interface.
 * SingleProjectQueryEngine: extends CommonsQueryEngine. It should be used when the connector does not support joins, so there would be only one project.


In order to use these modules, it is necessary to create an implementation of the following classes included in this project:

 * Connection
 * ConnectionHandler: This handler is in charge of the connector connections. It allows to recover, disconnect and store connections. The connection to handle should fulfill the Connection interface.

Usage examples can be found in connectors using this features: 

 * [MongoDB](https://github.com/Stratio/stratio-connector-mongodb)
 * [Streaming](https://github.com/Stratio/stratio-connector-streaming)
 * [HDFS](https://github.com/Stratio/stratio-connector-hdfs)
 * [Elasticsearch](https://github.com/Stratio/stratio-connector-elasticsearch)
 * [Deep-Spark](https://github.com/Stratio/stratio-connector-deep)
 * [Aerospike](https://github.com/Stratio/stratio-connector-aerospike)

Functional tests
---------------

This API includes a set of functional tests that accomplish the main body of Crossdata features. To run these generic tests is needed to: 

 * Extend the functional test. It is possible both override or ignore the test which features are not implemented. 

 * Implement a ConnectorHelper. Here, the cluster configuration and the database specific properties are defined. Therefore, you could use different implementations to probe the tests with the desired configuration.

The list of functional tests implemented is detailed [here](_doc/FunctionalTests.md).

Utilities
---------

Helper classes for simplifying testing:

 * LogicalWorkflowCreator: use to create LogicalWorkflow
 * CatalogMetadataBuilder: used to create CatalogMetadata
 * TableMetadataBuilder: used to create TableMetadata
 * IndexMetadataBuilder: used to create IndexMetadata


Common processing for dealing with Crossdata structures are included:

 * ManifestUtil: Parses the Connector manifest to extract not only the connector but the datastore name. It should be used to allow different connector names in the configuration. 
 * SelectorHelper: Allows the casting of Crossdata Selectors in order to retrieve the proper value.
 * ProjectParsed: Parses and validates the Crossdata logical steps from reading the Project's workflow. 
 * ColumnTypeHelper: Allows the casting of an object based on the columntType.
 * ConnectorParser: Parses the raw information related to hosts and ports coming from Crossdata to the format required by the connector.

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





