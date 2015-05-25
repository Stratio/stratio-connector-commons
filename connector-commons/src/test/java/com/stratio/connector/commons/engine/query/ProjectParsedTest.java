/*
 * Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */
package com.stratio.connector.commons.engine.query;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.*;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.statements.structures.window.WindowType;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;

/**
 * ProjectParsed Tester.
 *
 * @author <Authors name>
 * @version 1.0
 * @since <pre>
 * dic 16, 2014
 * </pre>
 */
public class ProjectParsedTest {

    private static final String CATALOG = "catalog";
    private static final String TABLE = "table";
    private static final TableName TABLE_NAME = new TableName(CATALOG, TABLE);
    private static final String CLUSTER = "cluster";
    private static final ClusterName CLUSTER_NAME = new ClusterName(CLUSTER);

    /**
     * Method: hasProjection()
     */
    @Test
    public void testCreateProjecParsedProject() throws Exception {
        Set<Operations> operation = new HashSet<>();
        operation.add(Operations.PROJECT);
        Project project = new Project(operation, TABLE_NAME, CLUSTER_NAME);

        ProjectParsed projectParsed = new ProjectParsed(project, mock(ProjectValidator.class));
        assertTrue("The filter list must be empty", projectParsed.getFilter().isEmpty());
        assertNull("The limit must be null", projectParsed.getLimit());
        assertTrue("The filter match must be empty", projectParsed.getFilter().isEmpty());
        assertNull("The select must be null", projectParsed.getSelect());
        assertNull("The groupBy must be null", projectParsed.getGroupBy());
        assertNull("The orderBy must be null", projectParsed.getGroupBy());
        assertEquals("The project must be the project pass in constructor", project, projectParsed.getProject());

    }

    @Test
    public void testCreateProjecParsedProjectSelect() throws Exception {

        Set<Operations> operationProject = new HashSet<>();
        operationProject.add(Operations.PROJECT);
        Project project = new Project(operationProject, TABLE_NAME, CLUSTER_NAME);
        Set<Operations> selectOperation = new HashSet<>();
        selectOperation.add(Operations.SELECT_OPERATOR);
        Select select = new Select(selectOperation, null, null, null);
        project.setNextStep(select);

        ProjectParsed projectParsed = new ProjectParsed(project, mock(ProjectValidator.class));
        assertTrue("The filter list must be empty", projectParsed.getFilter().isEmpty());
        assertNull("The limit must be null", projectParsed.getLimit());
        assertNull("The window must be null", projectParsed.getWindow());
        assertTrue("The filter match list be empty", projectParsed.getMatchList().isEmpty());
        assertEquals("The select must be the select created before", select, projectParsed.getSelect());
        assertNull("The groupBy must be null", projectParsed.getGroupBy());
        assertEquals("The project must be the project pass in constructor", project, projectParsed.getProject());

    }

    @Test
    public void testCreateProjecParsedProjecFilter() throws Exception {
        Set<Operations> projectOperation = new HashSet<>();
        projectOperation.add(Operations.PROJECT);
        Project project = new Project(projectOperation, TABLE_NAME, CLUSTER_NAME);


        Relation relation = new Relation(null, Operator.EQ, null);
        Set<Operations> filterOperation = new HashSet<>();
        filterOperation.add(Operations.FILTER_INDEXED_EQ);
        Filter filter = new Filter(filterOperation, relation);
        project.setNextStep(filter);

        ProjectParsed projectParsed = new ProjectParsed(project, mock(ProjectValidator.class));

        Collection<Filter> filters = projectParsed.getFilter();
        assertEquals("The filter list must one element", 1, filters.size());
        assertEquals("The filter element must be the filter created before", filter, filters.toArray()[0]);

        assertNull("The limit must be null", projectParsed.getLimit());
        assertTrue("The filter match list be empty", projectParsed.getMatchList().isEmpty());
        assertNull("The window must be null", projectParsed.getWindow());
        assertNull("The select must be null", projectParsed.getSelect());
        assertNull("The groupBy must be null", projectParsed.getGroupBy());
        assertEquals("The project must be the project pass in constructor", project, projectParsed.getProject());

    }

    @Test
    public void testCreateProjecParsedProjecMatch() throws Exception {
        Set<Operations> projectOperation = new HashSet<>();
        projectOperation.add(Operations.PROJECT);
        Project project = new Project(projectOperation, TABLE_NAME, CLUSTER_NAME);

        Set<Operations> filterOperation = new HashSet<>();
        filterOperation.add(Operations.FILTER_INDEXED_MATCH);
        Relation relation = new Relation(null, Operator.MATCH, null);
        Filter filter = new Filter(filterOperation, relation);
        project.setNextStep(filter);

        ProjectValidator projectValidator = mock(ProjectValidator.class);
        ProjectParsed projectParsed = new ProjectParsed(project, projectValidator);

        assertTrue("The filter list must be empty", projectParsed.getFilter().isEmpty());

        Collection<Filter> match = projectParsed.getMatchList();
        assertEquals("The filter match list must one element", 1, match.size());
        assertEquals("The filter match  element must be the filter created before", filter, match.toArray()[0]);

        assertNull("The limit must be null", projectParsed.getLimit());
        assertNull("The window must be null", projectParsed.getWindow());

        assertNull("The select must be null", projectParsed.getSelect());
        assertNull("The groupBy must be null", projectParsed.getGroupBy());
        assertEquals("The project must be the project pass in constructor", project, projectParsed.getProject());

    }

    @Test
    public void testCreateProjecParsedProjectLimit() throws Exception {
        Set<Operations> projectOperation = new HashSet<>();
        projectOperation.add(Operations.PROJECT);
        Project project = new Project(projectOperation, TABLE_NAME, CLUSTER_NAME);
        Set<Operations> selectOperation = new HashSet<>();
        selectOperation.add(Operations.SELECT_LIMIT);
        Limit limit = new Limit(selectOperation, 10);
        project.setNextStep(limit);

        ProjectParsed projectParsed = new ProjectParsed(project, mock(ProjectValidator.class));
        assertTrue("The filter list must be empty", projectParsed.getFilter().isEmpty());
        assertEquals("The limit must be the limit created before", limit, projectParsed.getLimit());
        assertTrue("The filter match list be empty", projectParsed.getMatchList().isEmpty());
        assertNull("The select must be null", projectParsed.getSelect());
        assertNull("The window must be null", projectParsed.getWindow());
        assertNull("The groupBy must be null", projectParsed.getGroupBy());
        assertEquals("The project must be the project pass in constructor", project, projectParsed.getProject());

    }

    @Test
    public void testCreateProjecParsedProjectWindow() throws Exception {
        Set<Operations> projectOperation = new HashSet<>();
        projectOperation.add(Operations.PROJECT);
        Project project = new Project(projectOperation, TABLE_NAME, CLUSTER_NAME);
        Set<Operations> selectOperation = new HashSet<>();
        selectOperation.add(Operations.SELECT_WINDOW);
        Window window = new Window(selectOperation, WindowType.NUM_ROWS);

        project.setNextStep(window);

        ProjectParsed projectParsed = new ProjectParsed(project, mock(ProjectValidator.class));
        assertTrue("The filter list must be empty", projectParsed.getFilter().isEmpty());
        assertNull("The limit must be null", projectParsed.getLimit());
        assertNull("The select must be null", projectParsed.getSelect());
        assertEquals("The window must be the window created before", window, projectParsed.getWindow());
        assertNull("The groupBy must be null", projectParsed.getGroupBy());
        assertEquals("The project must be the project pass in constructor", project, projectParsed.getProject());

    }

    @Test
    public void testCreateProjecParsedProjectGroupBy() throws Exception {
        Set<Operations> projectOperation = new HashSet<>();
        projectOperation.add(Operations.PROJECT);
        Project project = new Project(projectOperation, TABLE_NAME, CLUSTER_NAME);
        Set<Operations> selectOperation = new HashSet<>();
        selectOperation.add(Operations.SELECT_GROUP_BY);
        GroupBy groupBy = new GroupBy(selectOperation, null);

        project.setNextStep(groupBy);

        ProjectParsed projectParsed = new ProjectParsed(project, mock(ProjectValidator.class));
        assertTrue("The filter list must be empty", projectParsed.getFilter().isEmpty());
        assertNull("The limit must be null", projectParsed.getLimit());
        assertNull("The select must be null", projectParsed.getSelect());
        assertNull("The window must be null", projectParsed.getWindow());
        assertEquals("The groupBy must be the groupBy created before", groupBy, projectParsed.getGroupBy());
        assertEquals("The project must be the project pass in constructor", project, projectParsed.getProject());

    }

    @Test
    public void testCreateProjecParsedProjectOrderBy() throws Exception {
        Set<Operations> projectOperation = new HashSet<>();
        projectOperation.add(Operations.PROJECT);
        Project project = new Project(projectOperation, TABLE_NAME, CLUSTER_NAME);
        Set<Operations> selectOperator = new HashSet<>();
        selectOperator.add(Operations.SELECT_ORDER_BY);
        OrderBy orderBy = new OrderBy(selectOperator, null);

        project.setNextStep(orderBy);

        ProjectParsed projectParsed = new ProjectParsed(project, mock(ProjectValidator.class));
        assertTrue("The filter list must be empty", projectParsed.getFilter().isEmpty());
        assertNull("The limit must be null", projectParsed.getLimit());
        assertNull("The select must be null", projectParsed.getSelect());
        assertNull("The window must be null", projectParsed.getWindow());
        assertNull("The groupBy must be null", projectParsed.getGroupBy());
        assertEquals("The orderBy must be the orderBy created before", Operations.SELECT_ORDER_BY, projectParsed
                .getOrderBy().getOperations().toArray()[0]);
        assertEquals("The project must be the project pass in constructor", project, projectParsed.getProject());

    }

}
