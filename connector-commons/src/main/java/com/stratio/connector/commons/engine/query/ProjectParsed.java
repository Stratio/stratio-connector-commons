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

import com.stratio.crossdata.common.exceptions.ConnectorException;
import com.stratio.crossdata.common.exceptions.ExecutionException;
import com.stratio.crossdata.common.logicalplan.*;
import com.stratio.crossdata.common.statements.structures.FunctionSelector;
import com.stratio.crossdata.common.statements.structures.Operator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * This class is a representation of a query.
 * <p/>
 * <p/>
 * Created by jmgomez on 15/09/14.
 */
public class ProjectParsed {

    /**
     * The Log.
     */
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    /**
     * The project.
     */
    private Project project = null;

    /**
     * The filters.
     */
    private Collection<Filter> filterList = Collections.emptyList();

    /**
     * The disjunctions (OR trees).
     */
    private Collection<Disjunction> disjunctionList = Collections.emptyList();

    /**
     * The matchList.
     */
    private Collection<Filter> matchList = Collections.emptyList();

    /**
     * The functionFilters.
     */
    private Collection<FunctionFilter> functionFilters = new ArrayList<>();

    /**
     * The disjunctions of functionFilters (OR trees).
     */
    private Collection<Disjunction> disjunctionOfFunctionsList = Collections.emptyList();

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
     * The orderBy.
     */
    private OrderBy orderBy;


    /**
     * Constructor.
     *
     * @param project          the project.
     * @param projectValidator the validator for the projecto.
     * @throws ConnectorException if the logical step is not supported.
     */
    public ProjectParsed(Project project, ProjectValidator projectValidator) throws ConnectorException {
        this.project = project;
        LogicalStep lStep = project;
        while ((lStep = lStep.getNextStep()) != null) {
            addLogicalStep(lStep);
        }
        projectValidator.validate(this);
    }

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
     * Return The functionFilters.
     *
     * @return the functionFilters
     */
    public Collection<FunctionFilter> getFunctionFilters() {
        return new ArrayList(functionFilters);
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
     * Return the GroupBy.
     *
     * @return the groupBy.
     */
    public GroupBy getGroupBy() {
        return groupBy;
    }

    /**
     * Return the OrderBy.
     *
     * @return the orderBy.
     */
    public OrderBy getOrderBy() {
        return orderBy;
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
     *
     * @param lStep logical step.
     * @throws ExecutionException if the logical step is not supported.
     */
    private void addLogicalStep(LogicalStep lStep) throws ExecutionException {
        if (lStep instanceof Project) {
            project = (Project) lStep;

        } else if (lStep instanceof Filter) {
            decideTypeFilterToAdd((Filter) lStep);

        } else if (lStep instanceof FunctionFilter){
            functionFilters.add((FunctionFilter) lStep);

        }else if (lStep instanceof Select) {
            select = (Select) lStep;

        } else if (lStep instanceof Limit) {
            limit = (Limit) lStep;

        } else if (lStep instanceof GroupBy) {
            groupBy = (GroupBy) lStep;

        } else if (lStep instanceof Window) {
            window = (Window) lStep;

        } else if (lStep instanceof OrderBy) {
            orderBy = (OrderBy) lStep;
        }else if (lStep instanceof Disjunction){
            switchDisjunctionList((Disjunction) lStep);
        } else {

            String message = "LogicalStep [" + lStep.getClass().getCanonicalName() + " not supported";
            logger.error(message);
            throw new ExecutionException(message);
        }
    }

    public void switchDisjunctionList(Disjunction disjunction) throws ExecutionException {

        if (isPureFunctionsDisjunction(disjunction)){
            if (disjunctionOfFunctionsList.isEmpty()) {
                disjunctionOfFunctionsList = new ArrayList<>();
            }
            disjunctionOfFunctionsList.add(disjunction);

        }else if (isPureFilterDisjunction(disjunction)) {

            if (disjunctionList.isEmpty()) {
                disjunctionList = new ArrayList<>();
            }
            disjunctionList.add(disjunction);
        }else{
            String message = "Mixing FunctionFilter with Filter using OR disjunction is not supported Yet...";
            logger.error(message);
            throw new ExecutionException(message);
        }
    }

    public boolean isPureFunctionsDisjunction(Disjunction disjunction){

        for (List<ITerm> termsList :disjunction.getTerms()){
            for (ITerm term : termsList){
                if (term instanceof  Disjunction
                        && !isPureFunctionsDisjunction((Disjunction) term)){
                    return false;
                }
                if (term instanceof Filter){
                    return false;
                }
            }
        }
        return true;
    }

    public boolean isPureFilterDisjunction(Disjunction disjunction){
        for (List<ITerm> termsList :disjunction.getTerms()){
            for (ITerm term : termsList){
                if (term instanceof  Disjunction
                        && !isPureFilterDisjunction((Disjunction) term)){
                    return false;
                }
                if (term instanceof FunctionFilter){
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Add filter in the correct list.
     *
     * @param filter the filter.
     */
    private void decideTypeFilterToAdd(Filter filter) {
        Filter step = filter;

        if (Operator.MATCH == step.getRelation().getOperator() ||
                step.getRelation().getRightTerm() instanceof FunctionSelector) {
            if (matchList.isEmpty()) {
                matchList = new ArrayList<>();
            }
            matchList.add(filter);
        } else {
            if (filterList.isEmpty()) {
                filterList = new ArrayList<>();
            }
            filterList.add(filter);
        }
    }

    public Collection<Disjunction> getDisjunctionList() {
        return disjunctionList;
    }

    public Collection<Disjunction> getDisjunctionOfFunctionsList() {
        return disjunctionOfFunctionsList;
    }
}
