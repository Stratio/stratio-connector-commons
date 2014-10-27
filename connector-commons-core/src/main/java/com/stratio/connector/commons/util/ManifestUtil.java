/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional information
 * regarding copyright ownership.  The STRATIO (C) licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.stratio.connector.commons.util;

import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.stratio.crossdata.common.exceptions.InitializationException;

/**
 * Utilities for manifest.
 * Created by jmgomez on 27/10/14.
 */
public final class ManifestUtil {

    /**
     * The Log.
     */
    final transient static Logger logger = LoggerFactory.getLogger(ManifestUtil.class);

    /**
     * Constructor.
     */
    private ManifestUtil() {
    }

    /**
     * Recovered the datastoreName form Manifest.
     *
     * @param pathManifest the manifest path.
     * @return the datastoreName.
     * @throws InitializationException if an error happens while XML is reading.
     */
    public static String[] getDatastoreName(String pathManifest) throws InitializationException {

        String[] datastoreName = { "" };
        try {
            Document document = getDocument(pathManifest);
            //Search for the limit properties and connectorName
            Object result = getResult(document, "//DataStores/DataStoreName/text()");

            datastoreName = new String[((NodeList) result).getLength()];
            for (int i = 0; i < ((NodeList) result).getLength(); i++) {
                datastoreName[i] = ((NodeList) result).item(i).getNodeValue();
            }
        } catch (SAXException | XPathExpressionException | IOException | ParserConfigurationException e) {
            String msg =
                    "Impossible to read DataStoreName in Manifest with the connector configuration." + e.getCause();
            logger.error(msg);
            throw new InitializationException(msg, e);
        }

        return datastoreName;
    }

    /**
     * Recovered the ConecrtorName form Manifest.
     *
     * @param pathManifest the manifest path.
     * @return the ConectionName.
     * @throws InitializationException if an error happens while XML is reading.
     */
    public static String getConectorName(String pathManifest) throws InitializationException {

        String connectionName = "";
        try {
            Document document = getDocument(pathManifest);

            Object result = getResult(document, "//ConnectorName/text()");
            connectionName = ((NodeList) result).item(0).getNodeValue();

        } catch (SAXException | XPathExpressionException | IOException | ParserConfigurationException e) {
            String msg =
                    "Impossible to read DataStoreName in Manifest with the connector configuration." + e.getCause();
            logger.error(msg);
            throw new InitializationException(msg, e);
        }

        return connectionName;
    }

    /**
     * Get the node value.
     *
     * @param document the document.
     * @param node     the node.
     * @return the node value.
     * @throws XPathExpressionException if an exception happens.
     */
    private static Object getResult(Document document, String node) throws XPathExpressionException {
        // create an XPath object
        XPath xpath = XPathFactory.newInstance().newXPath();
        Object result;
        XPathExpression expr = null;
        expr = xpath.compile(node);
        result = expr.evaluate(document, XPathConstants.NODESET);
        return result;
    }

    /**
     * Create the documento.
     *
     * @param pathManifest the manifest Path.
     * @return the document.
     * @throws SAXException                 if an exception happens.
     * @throws IOException                  if an exception happens.
     * @throws ParserConfigurationException if an exception happens.
     */
    private static Document getDocument(String pathManifest)
            throws SAXException, IOException, ParserConfigurationException {
        InputStream inputStream =
                ManifestUtil.class.getClassLoader().getResourceAsStream(pathManifest);
        return DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputStream);
    }

}



