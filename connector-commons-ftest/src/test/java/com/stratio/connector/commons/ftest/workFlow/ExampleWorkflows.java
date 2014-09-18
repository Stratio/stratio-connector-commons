/*
 * Stratio Deep
 *
 *   Copyright (c) 2014, Stratio, All rights reserved.
 *
 *   This library is free software; you can redistribute it and/or modify it under the terms of the
 *   GNU Lesser General Public License as published by the Free Software Foundation; either version
 *   3.0 of the License, or (at your option) any later version.
 *
 *   This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 *   even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 *   Lesser General Public License for more details.
 *
 *   You should have received a copy of the GNU Lesser General Public License along with this library.
 */

package com.stratio.connector.commons.ftest.workFlow;

import com.stratio.meta.common.connector.Operations;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.logicalplan.*;
import com.stratio.meta.common.statements.structures.relationships.Operator;
import com.stratio.meta.common.statements.structures.relationships.Relation;
import com.stratio.meta2.common.data.ColumnName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.statements.structures.selectors.ColumnSelector;
import com.stratio.meta2.common.statements.structures.selectors.IntegerSelector;
import com.stratio.meta2.common.statements.structures.selectors.Selector;
import com.stratio.meta2.common.statements.structures.selectors.StringSelector;

import java.util.*;

/**
 * Example workflows to test basic functionality of the different connectors.
 * This class assumes the existence of a catalog named {@code example} and with
 * two tables named {@code users}, and {@code information}.
 *
 * Table {@code users} contains the following fields:
 * <li>
 *   <ul>id: integer, PK</ul>
 *   <ul>name: text, PK, indexed</ul>
 *   <ul>age: integer</ul>
 *   <ul>bool: boolean</ul>
 * </li>
 *
 * Table {@code information} contains the following fields:
 * <li>
 *   <ul>id: integer, PK</ul>
 *   <ul>phrase: text, indexed</ul>
 *   <ul>email: text</ul>
 *   <ul>score: double</ul>
 * </li>
 */
public class ExampleWorkflows {

    public static final String COLUMN_ID = "id";
    public static final String COLUMN_NAME = "name";
    public static final String COLUMN_AGE = "age";
    public static final String COLUMN_BOOL = "bool";
    public static final String ALIAS_ID = "users.id";
    public static final String ALIAS_NAME = "users.name";
    public static final String ALIAS_AGE = "users.age";

    public String table;
    public String catalog;

    public ExampleWorkflows(String catalog, String table) {
        this.catalog = catalog;
        this.table = table;
    }

    public static String[] names ={
            "MARY", "PATRICIA", "LINDA", "BARBARA", "ELIZABETH", "JENNIFER", "MARIA", "SUSAN", "MARGARET", "DOROTHY", "LISA", "NANCY", "KAREN", "BETTY", "HELEN", "SANDRA", "DONNA", "CAROL", "RUTH", "SHARON", "MICHELLE", "LAURA", "SARAH", "KIMBERLY", "DEBORAH", "JESSICA", "SHIRLEY", "CYNTHIA", "ANGELA", "MELISSA", "BRENDA", "AMY", "ANNA", "REBECCA", "VIRGINIA", "KATHLEEN", "PAMELA", "MARTHA", "DEBRA", "AMANDA", "STEPHANIE", "CAROLYN", "CHRISTINE", "MARIE", "JANET", "CATHERINE", "FRANCES", "ANN", "JOYCE", "DIANE", "ALICE", "JULIE", "HEATHER", "TERESA", "DORIS", "GLORIA", "EVELYN", "JEAN", "CHERYL", "MILDRED", "KATHERINE", "JOAN", "ASHLEY", "JUDITH", "ROSE", "JANICE", "KELLY", "NICOLE", "JUDY", "CHRISTINA", "KATHY", "THERESA", "BEVERLY", "DENISE", "TAMMY", "IRENE", "JANE", "LORI", "RACHEL", "MARILYN", "ANDREA", "KATHRYN", "LOUISE", "SARA", "ANNE", "JACQUELINE", "WANDA", "BONNIE", "JULIA", "RUBY", "LOIS", "TINA", "PHYLLIS", "NORMA", "PAULA", "DIANA", "ANNIE", "LILLIAN", "EMILY", "ROBIN", "PEGGY", "CRYSTAL", "GLADYS", "RITA", "DAWN", "CONNIE", "FLORENCE", "TRACY", "EDNA", "TIFFANY", "CARMEN", "ROSA", "CINDY", "GRACE", "WENDY", "VICTORIA", "EDITH", "KIM", "SHERRY", "SYLVIA", "JOSEPHINE", "THELMA", "SHANNON", "SHEILA", "ETHEL", "ELLEN", "ELAINE", "MARJORIE", "CARRIE", "CHARLOTTE", "MONICA", "ESTHER", "PAULINE", "EMMA", "JUANITA", "ANITA", "RHONDA", "HAZEL", "AMBER", "EVA", "DEBBIE", "APRIL", "LESLIE", "CLARA", "LUCILLE", "JAMIE", "JOANNE", "ELEANOR", "VALERIE", "DANIELLE", "MEGAN", "ALICIA", "SUZANNE", "MICHELE", "GAIL", "BERTHA", "DARLENE", "VERONICA", "JILL", "ERIN", "GERALDINE", "LAUREN", "CATHY", "JOANN", "LORRAINE", "LYNN", "SALLY", "REGINA", "ERICA", "BEATRICE", "DOLORES", "BERNICE", "AUDREY", "YVONNE", "ANNETTE", "JUNE", "SAMANTHA", "MARION", "DANA", "STACY", "ANA", "RENEE", "IDA", "VIVIAN", "ROBERTA", "HOLLY", "BRITTANY", "MELANIE", "LORETTA", "YOLANDA", "JEANETTE", "LAURIE", "KATIE", "KRISTEN", "VANESSA", "ALMA", "SUE", "ELSIE", "BETH", "JEANNE", "VICKI", "CARLA", "TARA", "ROSEMARY", "EILEEN", "TERRI", "GERTRUDE", "LUCY", "TONYA", "ELLA", "STACEY", "WILMA", "GINA", "KRISTIN", "JESSIE", "NATALIE", "AGNES", "VERA", "WILLIE", "CHARLENE", "BESSIE", "DELORES", "MELINDA", "PEARL", "ARLENE", "MAUREEN", "COLLEEN", "ALLISON", "TAMARA", "JOY", "GEORGIA", "CONSTANCE", "LILLIE", "CLAUDIA", "JACKIE", "MARCIA", "TANYA", "NELLIE", "MINNIE", "MARLENE", "HEIDI", "GLENDA", "LYDIA", "VIOLA", "COURTNEY", "MARIAN", "STELLA", "CAROLINE", "DORA", "JO", "VICKIE", "MATTIE", "TERRY", "MAXINE", "IRMA", "MABEL", "MARSHA", "MYRTLE", "LENA", "CHRISTY", "DEANNA", "PATSY", "HILDA", "GWENDOLYN", "JENNIE",
            "JAMES", "JOHN", "ROBERT", "MICHAEL", "WILLIAM", "DAVID", "RICHARD", "CHARLES", "JOSEPH", "THOMAS", "CHRISTOPHER", "DANIEL", "PAUL", "MARK", "DONALD", "GEORGE", "KENNETH", "STEVEN", "EDWARD", "BRIAN", "RONALD", "ANTHONY", "KEVIN", "JASON", "MATTHEW", "GARY", "TIMOTHY", "JOSE", "LARRY", "JEFFREY", "FRANK", "SCOTT", "ERIC", "STEPHEN", "ANDREW", "RAYMOND", "GREGORY", "JOSHUA", "JERRY", "DENNIS", "WALTER", "PATRICK", "PETER", "HAROLD", "DOUGLAS", "HENRY", "CARL", "ARTHUR", "RYAN", "ROGER", "JOE", "JUAN", "JACK", "ALBERT", "JONATHAN", "JUSTIN", "TERRY", "GERALD", "KEITH", "SAMUEL", "WILLIE", "RALPH", "LAWRENCE", "NICHOLAS", "ROY", "BENJAMIN", "BRUCE", "BRANDON", "ADAM", "HARRY", "FRED", "WAYNE", "BILLY", "STEVE", "LOUIS", "JEREMY", "AARON", "RANDY", "HOWARD", "EUGENE", "CARLOS", "RUSSELL", "BOBBY", "VICTOR", "MARTIN", "ERNEST", "PHILLIP", "TODD", "JESSE", "CRAIG", "ALAN", "SHAWN", "CLARENCE", "SEAN", "PHILIP", "CHRIS", "JOHNNY", "EARL", "JIMMY", "ANTONIO", "DANNY", "BRYAN", "TONY", "LUIS", "MIKE", "STANLEY", "LEONARD", "NATHAN", "DALE", "MANUEL", "RODNEY", "CURTIS", "NORMAN", "ALLEN", "MARVIN", "VINCENT", "GLENN", "JEFFERY", "TRAVIS", "JEFF", "CHAD", "JACOB", "LEE", "MELVIN", "ALFRED", "KYLE", "FRANCIS", "BRADLEY", "JESUS", "HERBERT", "FREDERICK", "RAY", "JOEL", "EDWIN", "DON", "EDDIE", "RICKY", "TROY", "RANDALL", "BARRY", "ALEXANDER", "BERNARD", "MARIO", "LEROY", "FRANCISCO", "MARCUS", "MICHEAL", "THEODORE", "CLIFFORD", "MIGUEL", "OSCAR", "JAY", "JIM", "TOM", "CALVIN", "ALEX", "JON", "RONNIE", "BILL", "LLOYD", "TOMMY", "LEON", "DEREK", "WARREN", "DARRELL", "JEROME", "FLOYD", "LEO", "ALVIN", "TIM", "WESLEY", "GORDON", "DEAN", "GREG", "JORGE", "DUSTIN", "PEDRO", "DERRICK", "DAN", "LEWIS", "ZACHARY", "COREY", "HERMAN", "MAURICE", "VERNON", "ROBERTO", "CLYDE", "GLEN", "HECTOR", "SHANE", "RICARDO", "SAM", "RICK", "LESTER", "BRENT", "RAMON", "CHARLIE", "TYLER", "GILBERT", "GENE", "MARC", "REGINALD", "RUBEN", "BRETT", "ANGEL", "NATHANIEL", "RAFAEL", "LESLIE", "EDGAR", "MILTON", "RAUL", "BEN", "CHESTER", "CECIL", "DUANE", "FRANKLIN", "ANDRE", "ELMER", "BRAD", "GABRIEL", "RON", "MITCHELL", "ROLAND", "ARNOLD", "HARVEY", "JARED", "ADRIAN", "KARL", "CORY", "CLAUDE", "ERIK", "DARRYL", "JAMIE", "NEIL", "JESSIE", "CHRISTIAN", "JAVIER", "FERNANDO", "CLINTON", "TED", "MATHEW", "TYRONE", "DARREN", "LONNIE", "LANCE", "CODY", "JULIO", "KELLY", "KURT", "ALLAN", "NELSON", "GUY", "CLAYTON", "HUGH", "MAX", "DWAYNE", "DWIGHT", "ARMANDO", "FELIX", "JIMMIE", "EVERETT", "JORDAN", "IAN", "WALLACE", "KEN", "BOB", "JAIME", "CASEY", "ALFREDO", "ALBERTO", "DAVE", "IVAN", "JOHNNIE", "SIDNEY", "BYRON", "JULIAN", "ISAAC", "MORRIS", "CLIFTON", "WILLARD", "DARYL", "ROSS", "VIRGIL", "ANDY", "MARSHALL", "SALVADOR", "PERRY", "KIRK", "SERGIO", "MARION", "TRACY", "SETH", "KENT", "TERRANCE", "RENE", "EDUARDO", "TERRENCE", "ENRIQUE", "FREDDIE", "WADE", "AUSTIN", "STUART", "FREDRICK", "ARTURO", "ALEJANDRO", "JACKIE", "JOEY", "NICK", "LUTHER", "WENDELL", "JEREMIAH", "EVAN", "JULIUS", "DANA", "DONNIE", "OTIS", "SHANNON", "TREVOR", "OLIVER", "LUKE", "HOMER", "GERARD", "DOUG", "KENNY", "HUBERT", "ANGELO", "SHAUN", "LYLE", "MATT", "LYNN", "ALFONSO", "ORLANDO", "REX", "CARLTON", "ERNESTO", "CAMERON", "NEAL", "PABLO", "LORENZO", "OMAR", "WILBUR", "BLAKE", "GRANT", "HORACE", "RODERICK", "KERRY",
            "ABRAHAM", "WILLIS", "RICKEY", "JEAN", "IRA", "ANDRES", "CESAR", "JOHNATHAN", "MALCOLM", "RUDOLPH", "DAMON", "KELVIN", "RUDY", "PRESTON", "ALTON", "ARCHIE", "MARCO", "WM", "PETE", "RANDOLPH", "GARRY", "GEOFFREY", "JONATHON", "FELIPE", "BENNIE", "GERARDO", "ED", "DOMINIC", "ROBIN", "LOREN", "DELBERT", "COLIN", "GUILLERMO", "EARNEST", "LUCAS", "BENNY", "NOEL", "SPENCER", "RODOLFO", "MYRON", "EDMUND", "GARRETT", "SALVATORE", "CEDRIC", "LOWELL", "GREGG", "SHERMAN", "WILSON", "DEVIN", "SYLVESTER", "KIM", "ROOSEVELT", "ISRAEL", "JERMAINE", "FORREST", "WILBERT", "LELAND", "SIMON", "GUADALUPE", "CLARK", "IRVING", "CARROLL", "BRYANT", "OWEN", "RUFUS", "WOODROW", "SAMMY", "KRISTOPHER", "MACK", "LEVI", "MARCOS", "GUSTAVO", "JAKE", "LIONEL", "MARTY", "TAYLOR", "ELLIS", "DALLAS", "GILBERTO", "CLINT", "NICOLAS", "LAURENCE", "ISMAEL", "ORVILLE", "DREW", "JODY", "ERVIN", "DEWEY", "AL", "WILFRED", "JOSH", "HUGO", "IGNACIO", "CALEB", "TOMAS", "SHELDON", "ERICK", "FRANKIE", "STEWART", "DOYLE", "DARREL", "ROGELIO", "TERENCE", "SANTIAGO", "ALONZO", "ELIAS", "BERT", "ELBERT", "RAMIRO", "CONRAD", "PAT", "NOAH", "GRADY", "PHIL", "CORNELIUS", "LAMAR", "ROLANDO", "CLAY", "PERCY", "DEXTER", "BRADFORD", "MERLE", "DARIN", "AMOS", "TERRELL", "MOSES", "IRVIN", "SAUL", "ROMAN", "DARNELL", "RANDAL", "TOMMIE", "TIMMY", "DARRIN", "WINSTON", "BRENDAN", "TOBY", "VAN", "ABEL", "DOMINICK", "BOYD", "COURTNEY", "JAN", "EMILIO", "ELIJAH", "CARY", "DOMINGO", "SANTOS", "AUBREY", "EMMETT", "MARLON", "EMANUEL", "JERALD", "EDMOND", "EMIL", "DEWAYNE", "WILL", "OTTO", "TEDDY", "REYNALDO", "BRET", "MORGAN", "JESS", "TRENT", "HUMBERTO", "EMMANUEL", "STEPHAN", "LOUIE", "VICENTE", "LAMONT", "STACY", "GARLAND", "MILES", "MICAH", "EFRAIN", "BILLIE", "LOGAN", "HEATH", "RODGER", "HARLEY", "DEMETRIUS", "ETHAN", "ELDON", "ROCKY", "PIERRE", "JUNIOR", "FREDDY", "ELI", "BRYCE", "ANTOINE", "ROBBIE", "KENDALL", "ROYCE", "STERLING", "MICKEY", "CHASE", "GROVER", "ELTON", "CLEVELAND", "DYLAN", "CHUCK", "DAMIAN", "REUBEN", "STAN", "AUGUST", "LEONARDO", "JASPER", "RUSSEL", "ERWIN", "BENITO", "HANS", "MONTE", "BLAINE", "ERNIE", "CURT", "QUENTIN", "AGUSTIN", "MURRAY", "JAMAL", "DEVON", "ADOLFO", "HARRISON", "TYSON", "BURTON", "BRADY", "ELLIOTT", "WILFREDO", "BART", "JARROD", "VANCE", "DENIS", "DAMIEN", "JOAQUIN", "HARLAN", "DESMOND", "ELLIOT", "DARWIN", "ASHLEY", "GREGORIO", "BUDDY", "XAVIER", "KERMIT", "ROSCOE", "ESTEBAN", "ANTON", "SOLOMON", "SCOTTY", "NORBERT", "ELVIN", "WILLIAMS", "NOLAN", "CAREY", "ROD", "QUINTON", "HAL", "BRAIN", "ROB", "ELWOOD", "KENDRICK", "DARIUS", "MOISES", "SON", "MARLIN", "FIDEL", "THADDEUS", "CLIFF", "MARCEL", "ALI", "JACKSON", "RAPHAEL", "BRYON", "ARMAND", "ALVARO", "JEFFRY", "DANE", "JOESPH", "THURMAN", "NED", "SAMMIE", "RUSTY", "MICHEL", "MONTY", "RORY", "FABIAN", "REGGIE", "MASON", "GRAHAM", "KRIS", "ISAIAH", "VAUGHN", "GUS", "AVERY", "LOYD", "DIEGO", "ALEXIS", "ADOLPH", "NORRIS", "MILLARD", "ROCCO", "GONZALO", "DERICK", "RODRIGO", "GERRY", "STACEY", "CARMEN", "WILEY", "RIGOBERTO", "ALPHONSO", "TY", "SHELBY", "RICKIE", "NOE", "VERN", "BOBBIE", "REED", "JEFFERSON", "ELVIS", "BERNARDO", "MAURICIO", "HIRAM", "DONOVAN", "BASIL", "RILEY", "OLLIE", "NICKOLAS", "MAYNARD", "SCOT", "VINCE", "QUINCY", "EDDY", "SEBASTIAN", "FEDERICO", "ULYSSES", "HERIBERTO", "DONNELL", "COLE", "DENNY",
            "DAVIS", "GAVIN", "EMERY", "WARD", "ROMEO", "JAYSON", "DION", "DANTE", "CLEMENT", "COY", "ODELL", "MAXWELL", "JARVIS", "BRUNO", "ISSAC", "MARY", "DUDLEY", "BROCK", "SANFORD", "COLBY", "CARMELO", "BARNEY", "NESTOR", "HOLLIS", "STEFAN", "DONNY", "ART", "LINWOOD", "BEAU", "WELDON", "GALEN", "ISIDRO", "TRUMAN", "DELMAR", "JOHNATHON", "SILAS", "FREDERIC", "DICK", "KIRBY", "IRWIN", "CRUZ", "MERLIN", "MERRILL", "CHARLEY", "MARCELINO", "LANE", "HARRIS", "CLEO", "CARLO", "TRENTON", "KURTIS", "HUNTER", "AURELIO", "WINFRED", "VITO", "COLLIN", "DENVER", "CARTER", "LEONEL", "EMORY", "PASQUALE", "MOHAMMAD", "MARIANO", "DANIAL", "BLAIR", "LANDON", "DIRK", "BRANDEN", "ADAN", "NUMBERS", "CLAIR", "BUFORD", "GERMAN", "BERNIE", "WILMER", "JOAN", "EMERSON", "ZACHERY", "FLETCHER", "JACQUES", "ERROL", "DALTON", "MONROE", "JOSUE", "DOMINIQUE", "EDWARDO", "BOOKER", "WILFORD", "SONNY", "SHELTON", "CARSON", "THERON", "RAYMUNDO", "DAREN", "TRISTAN", "HOUSTON", "ROBBY", "LINCOLN", "JAME", "GENARO", "GALE", "BENNETT", "OCTAVIO", "CORNELL", "LAVERNE", "HUNG", "ARRON", "ANTONY", "HERSCHEL", "ALVA", "GIOVANNI", "GARTH", "CYRUS", "CYRIL", "RONNY", "STEVIE", "LON", "FREEMAN", "ERIN", "DUNCAN", "KENNITH", "CARMINE", "AUGUSTINE", "YOUNG", "ERICH", "CHADWICK", "WILBURN", "RUSS", "REID", "MYLES", "ANDERSON", "MORTON", "JONAS", "FOREST", "MITCHEL", "MERVIN", "ZANE", "RICH", "JAMEL", "LAZARO", "ALPHONSE", "RANDELL", "MAJOR", "JOHNIE", "JARRETT", "BROOKS", "ARIEL", "ABDUL", "DUSTY", "LUCIANO", "LINDSEY", "TRACEY", "SEYMOUR", "SCOTTIE", "EUGENIO", "MOHAMMED", "SANDY", "VALENTIN", "CHANCE", "ARNULFO", "LUCIEN", "FERDINAND", "THAD", "EZRA", "SYDNEY", "ALDO", "RUBIN", "ROYAL", "MITCH", "EARLE", "ABE", "WYATT", "MARQUIS", "LANNY", "KAREEM", "JAMAR", "BORIS", "ISIAH", "EMILE", "ELMO", "ARON", "LEOPOLDO", "EVERETTE", "JOSEF", "GAIL", "ELOY", "DORIAN", "RODRICK", "REINALDO", "LUCIO", "JERROD", "WESTON", "HERSHEL", "BARTON", "PARKER", "LEMUEL", "LAVERN", "BURT", "JULES", "GIL", "ELISEO", "AHMAD", "NIGEL", "EFREN", "ANTWAN", "ALDEN", "MARGARITO", "COLEMAN", "REFUGIO", "DINO", "OSVALDO", "LES", "DEANDRE", "NORMAND", "KIETH", "IVORY", "ANDREA", "TREY", "NORBERTO", "NAPOLEON", "JEROLD", "FRITZ", "ROSENDO", "MILFORD", "SANG", "DEON", "CHRISTOPER", "ALFONZO", "LYMAN", "JOSIAH", "BRANT", "WILTON", "RICO", "JAMAAL", "DEWITT", "CAROL", "BRENTON", "YONG", "OLIN", "FOSTER", "FAUSTINO", "CLAUDIO", "JUDSON", "GINO", "EDGARDO", "BERRY", "ALEC", "TANNER", "JARRED", "DONN", "TRINIDAD", "TAD", "SHIRLEY", "PRINCE", "PORFIRIO", "ODIS", "MARIA", "LENARD", "CHAUNCEY", "CHANG", "TOD", "MEL", "MARCELO", "KORY", "AUGUSTUS", "KEVEN", "HILARIO", "BUD", "SAL", "ROSARIO", "ORVAL", "MAURO", "DANNIE", "ZACHARIAH", "OLEN", "ANIBAL", "MILO", "JED", "FRANCES", "THANH", "DILLON", "AMADO", "NEWTON", "CONNIE"};


    public static Boolean[] booleans = {true,false};

    public List<Row> getRows(int init) {

        List<Row> rows = new ArrayList<Row>();

        for (int i = init*10000; i < (init+1)*10000; i++) {
            Row row = new Row();
            row.addCell(COLUMN_ID, new Cell(i));
            row.addCell(COLUMN_NAME, new Cell(names[i % names.length]));
            row.addCell(COLUMN_AGE, new Cell(i % 111));
            row.addCell(COLUMN_BOOL, new Cell(booleans[i % booleans.length]));
            rows.add(row);
            System.out.println("Row ["+i+"]");
        }
        return rows;
    }

  /**
   * Get a project operator taking the table name from the first column. This operation
   * assumes all columns belong to the same table.
   * @param columnNames The list of columns.
   * @return A {@link com.stratio.meta.common.logicalplan.Project}.
   */
  public Project getProject(ColumnName ... columnNames){
    TableName table = new TableName(
        columnNames[0].getTableName().getCatalogName().getName(),
        columnNames[0].getTableName().getName());
    return new Project(Operations.PROJECT, table, Arrays.asList(columnNames));
  }

  /**
   * Get a select operator.
   * @param alias The alias
   * @param columnNames The list of columns.
   * @return A {@link com.stratio.meta.common.logicalplan.Select}.
   */
  public Select getSelect(String [] alias, ColumnName ... columnNames){
    Map<String, String> columnMap = new HashMap<>();
    int aliasIndex = 0;
    for(ColumnName column : columnNames){
      columnMap.put(column.getQualifiedName(), alias[aliasIndex]);
      aliasIndex++;
    }
    return new Select(Operations.SELECT_OPERATOR, columnMap);
  }

  /**
   * Get a filter operator.
   * @param filterOp The Filter operation.
   * @param column The column name.
   * @param op The relationship operator.
   * @param right The right selector.
   * @return A {@link com.stratio.meta.common.logicalplan.Filter}.
   */
  public Filter getFilter(Operations filterOp, ColumnName column, Operator op, Selector right){
    Selector left = new ColumnSelector(column);
    Relation r = new Relation(left, op, right);
    return new Filter(filterOp, r);
  }


  /**
   * Get a basic select.
   * SELECT name, users.age FROM example.users;
   * @return A {@link com.stratio.meta.common.logicalplan.LogicalWorkflow}.
   */
  public LogicalWorkflow getBasicSelect(){
    ColumnName name = new ColumnName(table, catalog, "name");
    ColumnName age = new ColumnName(table, catalog, "age");
    String [] outputNames = {"name", ALIAS_AGE};
    LogicalStep project = getProject(name, age);
    LogicalStep select = getSelect(outputNames, name, age);
    project.setNextStep(select);
    LogicalWorkflow lw = new LogicalWorkflow(Arrays.asList(project));
    return lw;
  }

  /**
   * Get a basic select.
   * SELECT * FROM example.users;
   * @return A {@link com.stratio.meta.common.logicalplan.LogicalWorkflow}.
   */
  public LogicalWorkflow getBasicSelectAsterisk(){
    ColumnName id = new ColumnName(table, catalog, COLUMN_ID);
    ColumnName name = new ColumnName(table, catalog, COLUMN_NAME);
    ColumnName age = new ColumnName(table, catalog, COLUMN_AGE);
    ColumnName bool = new ColumnName(table, catalog, COLUMN_BOOL);
    String [] outputNames = {COLUMN_ID, COLUMN_NAME,COLUMN_AGE, COLUMN_BOOL};
    LogicalStep project = getProject(id, name, age, bool);
    LogicalStep select = getSelect(outputNames, id, name, age, bool);
    project.setNextStep(select);
    LogicalWorkflow lw = new LogicalWorkflow(Arrays.asList(project));
    return lw;
  }

  /**
   * Get a basic select with a single where clause on an indexed field.
   * SELECT users.id, users.name, users.age FROM example.users WHERE users.name='user1';
   * @return A {@link com.stratio.meta.common.logicalplan.LogicalWorkflow}.
   */
  public LogicalWorkflow getSelectIndexedField(){
    ColumnName id = new ColumnName(table, catalog, COLUMN_ID);
    ColumnName name = new ColumnName(table, catalog, COLUMN_NAME);
    ColumnName age = new ColumnName(table, catalog, COLUMN_AGE);
    String [] outputNames = {ALIAS_ID, ALIAS_NAME, ALIAS_AGE};
    LogicalStep project = getProject(id, name, age);
    LogicalStep filter = getFilter(Operations.FILTER_INDEXED_EQ,
                                   name, Operator.COMPARE, new StringSelector(names[0]));
    project.setNextStep(filter);
    LogicalStep select = getSelect(outputNames, name, age);
    filter.setNextStep(select);
    LogicalWorkflow lw = new LogicalWorkflow(Arrays.asList(project));
    return lw;
  }
/*
  /**
   * Get a basic select with a single where clause on a non-indexed field.
   * SELECT users.id, users.name, users.age FROM example.users WHERE users.age=42;
   * @return A {@link com.stratio.meta.common.logicalplan.LogicalWorkflow}.
   */
  public LogicalWorkflow getSelectNonIndexedField(){
    ColumnName id = new ColumnName(table, catalog, COLUMN_ID);
    ColumnName name = new ColumnName(table, catalog, COLUMN_NAME);
    ColumnName age = new ColumnName(table, catalog, COLUMN_AGE);
    String [] outputNames = {ALIAS_ID, ALIAS_NAME, ALIAS_AGE};
    LogicalStep project = getProject(id, name, age);
    LogicalStep filter = getFilter(Operations.FILTER_NON_INDEXED_EQ,
                                   age, Operator.COMPARE, new IntegerSelector(42));
    project.setNextStep(filter);
    LogicalStep select = getSelect(outputNames, name, age);
    filter.setNextStep(select);
    LogicalWorkflow lw = new LogicalWorkflow(Arrays.asList(project));
    return lw;
  }

  /**
   * Get a basic select with two where clauses.
   * SELECT users.id, users.name, users.age FROM example.users WHERE users.name='user1' AND users.age=42;
   * @return A {@link com.stratio.meta.common.logicalplan.LogicalWorkflow}.
   */
  public LogicalWorkflow getSelectMixedWhere(){
    ColumnName id = new ColumnName(table, catalog, COLUMN_ID);
    ColumnName name = new ColumnName(table, catalog, COLUMN_NAME);
    ColumnName age = new ColumnName(table, catalog, COLUMN_AGE);
    String [] outputNames = {ALIAS_ID, ALIAS_NAME, ALIAS_AGE};
    LogicalStep project = getProject(id, name, age);
    LogicalStep filterName = getFilter(Operations.FILTER_INDEXED_EQ,
                                   name, Operator.COMPARE, new StringSelector(names[1]));
    project.setNextStep(filterName);
    LogicalStep filterAge = getFilter(Operations.FILTER_NON_INDEXED_EQ,
                                   name, Operator.COMPARE, new IntegerSelector(42));
    filterName.setNextStep(filterAge);
    LogicalStep select = getSelect(outputNames, name, age);
    filterAge.setNextStep(select);
    LogicalWorkflow lw = new LogicalWorkflow(Arrays.asList(project));
    return lw;
  }

}
