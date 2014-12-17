package com.stratio.connector.commons.engine.query;

import static junit.framework.TestCase.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import com.stratio.crossdata.common.data.ClusterName;
import com.stratio.crossdata.common.data.TableName;
import com.stratio.crossdata.common.logicalplan.Filter;
import com.stratio.crossdata.common.logicalplan.Limit;
import com.stratio.crossdata.common.logicalplan.Project;
import com.stratio.crossdata.common.logicalplan.Select;
import com.stratio.crossdata.common.metadata.Operations;
import com.stratio.crossdata.common.statements.structures.Operator;
import com.stratio.crossdata.common.statements.structures.Relation;
import com.stratio.crossdata.common.logicalplan.Window;
import com.stratio.crossdata.common.statements.structures.window.WindowType;

/** 
* ProjectParsed Tester. 
* 
* @author <Authors name> 
* @since <pre>dic 16, 2014</pre> 
* @version 1.0 
*/ 
public class ProjectParsedTest {

   private static final String CATALOG = "catalog";
   private static final String TABLE = "table";
   private static final TableName TABLE_NAME = new TableName(CATALOG, TABLE);
   private static final String CLUSTER = "cluster";
   private static final ClusterName CLUSTER_NAME = new ClusterName(CLUSTER);

   /**
* 
* Method: hasProjection() 
* 
*/ 
@Test
public void testCreateProjecParsedProject() throws Exception {
   Project project = new Project(Operations.PROJECT,TABLE_NAME,CLUSTER_NAME);


   ProjectParsed projectParsed = new ProjectParsed(project);
   assertFalse("The filter list must be empty", projectParsed.getFilter().hasNext());
   assertNull("The limit must be null", projectParsed.getLimit());
   assertFalse("The filter match must be empty", projectParsed.getFilter().hasNext());
   assertNull("The select must be null", projectParsed.getSelect());
   assertEquals("The project must be the project pass in constructor", project, projectParsed.getProject());

}



   @Test
   public void testCreateProjecParsedProjectSelect() throws Exception {
      Project project = new Project(Operations.PROJECT,TABLE_NAME,CLUSTER_NAME);
      Select select = new Select(Operations.SELECT_OPERATOR,null,null,null);
      project.setNextStep(select);

      ProjectParsed projectParsed = new ProjectParsed(project);
      assertFalse("The filter list must be empty", projectParsed.getFilter().hasNext());
      assertNull("The limit must be null", projectParsed.getLimit());
      assertNull("The window must be null", projectParsed.getWindow());
      assertFalse("The filter match list be empty", projectParsed.getMatchList().hasNext());
      assertEquals("The select must be the select created before",select, projectParsed.getSelect());
      assertEquals("The project must be the project pass in constructor",project,projectParsed.getProject());

   }



   @Test
   public void testCreateProjecParsedProjecFilter() throws Exception {
      Project project = new Project(Operations.PROJECT,TABLE_NAME,CLUSTER_NAME);

      Relation relation = new Relation(null, Operator.EQ,null);
      Filter filter = new Filter(Operations.FILTER_INDEXED_EQ,relation);
      project.setNextStep(filter);

      ProjectParsed projectParsed = new ProjectParsed(project);

      Iterator<Filter> filterIterator = projectParsed.getFilter();
      assertTrue("The filter list must one element", filterIterator.hasNext());
      assertEquals("The filter element must be the filter created before", filter, filterIterator.next());
      assertFalse("The filter list must only one element", filterIterator.hasNext());

      assertNull("The limit must be null", projectParsed.getLimit());
      assertFalse("The filter match list be empty", projectParsed.getMatchList().hasNext());
      assertNull("The window must be null", projectParsed.getWindow());
      assertNull("The select must be null", projectParsed.getSelect());
      assertEquals("The project must be the project pass in constructor",project,projectParsed.getProject());

   }



   @Test
   public void testCreateProjecParsedProjecMatch() throws Exception {
      Project project = new Project(Operations.PROJECT,TABLE_NAME,CLUSTER_NAME);

      Relation relation = new Relation(null, Operator.MATCH,null);
      Filter filter = new Filter(Operations.FILTER_INDEXED_MATCH,relation);
      project.setNextStep(filter);

      ProjectParsed projectParsed = new ProjectParsed(project);

      assertFalse("The filter list must be empty", projectParsed.getFilter().hasNext());

      Iterator<Filter> matchIterator = projectParsed.getMatchList();
      assertTrue("The filter match list must one element", matchIterator.hasNext());
      assertEquals("The filter match  element must be the filter created before", filter, matchIterator
              .next());
      assertFalse("The filter match must only one element", matchIterator.hasNext());
      assertNull("The limit must be null", projectParsed.getLimit());
      assertNull("The window must be null", projectParsed.getWindow());

      assertNull("The select must be null", projectParsed.getSelect());
      assertEquals("The project must be the project pass in constructor",project,projectParsed.getProject());

   }


   @Test
   public void testCreateProjecParsedProjectLimit() throws Exception {
      Project project = new Project(Operations.PROJECT,TABLE_NAME,CLUSTER_NAME);
      Limit limit = new Limit(Operations.SELECT_LIMIT,10);
      project.setNextStep(limit);

      ProjectParsed projectParsed = new ProjectParsed(project);
      assertFalse("The filter list must be empty", projectParsed.getFilter().hasNext());
      assertEquals("The limit must be the limit created before", limit,projectParsed.getLimit());
      assertFalse("The filter match list be empty", projectParsed.getMatchList().hasNext());
      assertNull("The select must be null", projectParsed.getSelect());
      assertNull("The window must be null", projectParsed.getWindow());
      assertEquals("The project must be the project pass in constructor",project,projectParsed.getProject());

   }


   @Test
   public void testCreateProjecParsedProjectWindow() throws Exception {
      Project project = new Project(Operations.PROJECT,TABLE_NAME,CLUSTER_NAME);
      Window window = new Window(Operations.SELECT_WINDOW,WindowType.NUM_ROWS);

      project.setNextStep(window);

      ProjectParsed projectParsed = new ProjectParsed(project);
      assertFalse("The filter list must be empty", projectParsed.getFilter().hasNext());
      assertNull("The limit must be null", projectParsed.getLimit());
      assertNull("The select must be null", projectParsed.getSelect());
      assertEquals("The window must be the window created before", window, projectParsed.getWindow());
      assertEquals("The project must be the project pass in constructor",project,projectParsed.getProject());

   }


} 
