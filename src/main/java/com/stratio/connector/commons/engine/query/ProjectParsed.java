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

package com.stratio.connector.commons.engine.query;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

import com.stratio.crossdata.common.exceptions.UnsupportedException;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.GroupBy;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.LogicalStep;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.logicalplan.Window;
import com.stratio.crossdata.common.statements.structures.Operator;

/**
 * This class is a representation of a ElasticSearch query.
 * <p/>
 * <p/>
 * Created by jmgomez on 15/09/14.
 */
public class ProjectParsed {

    /**
     * Constructor.
     * @param project the project.
     * @param projectValidator the validator for the projecto.
     * @throws UnsupportedException if a logical step is not supported.
     */
    public ProjectParsed(Project project, ProjectValidator projectValidator) throws UnsupportedException {
        this.project = project;
        LogicalStep lStep =project;
        while((lStep = lStep.getNextStep())!=null) {
            addLogicalStep(lStep);
        }
        projectValidator.validate(this);
    }

    /**
     * The project.
     */
    private Project project = null;

    /**
     * The filters.
     */
    private Collection<Filter> filterList = Collections.EMPTY_LIST;
    /**
     * The matchList.
     */
    private Collection<Filter> matchList = Collections.EMPTY_LIST;

    /**
     * The select.
     */
    private Select select;
    /**
     * The limit.
     */
    private Limit limit;
    /**
     * The groupBy.
     */
    private GroupBy groupBy;
    /**
     * The window.
     */
    private Window window;




    /**
     * Get the project.
     *
     * @return the project,
     */
    public Project getProject() {

        return project;
    }



    /**
     * Get the filter.
     *
     * @return the filter.
     */
    public Collection<Filter> getFilter() {
        return new ArrayList(filterList);
    }



    /**
     * Return The matchList.
     *
     * @return the matchList
     */
    public Collection<Filter> getMatchList() {
        return new ArrayList(matchList);
    }



    /**
     * return the select.
     *
     * @return the select.
     */
    public Select getSelect() {
        return select;
    }


    /**
     * Return the limit.
     *
     * @return the limit.
     */
    public Window getWindow() {
        return window;
    }



    /**
     * Return the limit.
     *
     * @return the limit.
     */
    public Limit getLimit() {
        return limit;
    }

    /**
     * This method add the correct logical step.
     * @param lStep logical step.
     * @throws UnsupportedException if the logical step is not supported.
     */
    private void addLogicalStep(LogicalStep lStep) throws UnsupportedException {
        if (lStep instanceof Project){
            project =(Project)lStep;
        }else if (lStep instanceof Filter) {

            decideTypeFilterToAdd((Filter) lStep);

        }else if (lStep instanceof Select) {
            select=(Select)lStep;

        } else if (lStep instanceof Limit) {
            limit = (Limit)lStep;

        }else if (lStep instanceof GroupBy) {
            groupBy= (GroupBy) lStep;

        }else if (lStep instanceof Window){
            window=(Window) lStep;

        }else {
            throw new UnsupportedException(
                    "LogicalStep [" + lStep.getClass().getCanonicalName() + " not supported");
        }
    }

    /**
     * Add filter in the correct list.
     * @param filter the filter.
     */
    private void decideTypeFilterToAdd(Filter filter) {
        Filter step = filter;

        if (Operator.MATCH == step.getRelation().getOperator()) {
            if (matchList.isEmpty()){
                matchList = new ArrayList<>();
            }
            matchList.add(filter);
        } else {
            if (filterList.isEmpty()){
                filterList = new ArrayList<>();
            }
            filterList.add(filter);
        }
    }
}
