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

package com.stratio.connector.commons.ftest.functionalTestQuery;


import com.stratio.connector.commons.ftest.GenericConnectorTest;
import com.stratio.connector.commons.ftest.helper.LogicalWorkFlowCreator;
import com.stratio.meta.common.data.Cell;
import com.stratio.meta.common.data.ResultSet;
import com.stratio.meta.common.data.Row;
import com.stratio.meta.common.exceptions.ConnectionException;
import com.stratio.meta.common.exceptions.ExecutionException;
import com.stratio.meta.common.exceptions.InitializationException;
import com.stratio.meta.common.exceptions.UnsupportedException;
import com.stratio.meta.common.logicalplan.LogicalWorkflow;
import com.stratio.meta.common.result.QueryResult;
import com.stratio.meta2.common.data.ClusterName;
import com.stratio.meta2.common.data.TableName;
import com.stratio.meta2.common.metadata.TableMetadata;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static com.stratio.connector.commons.ftest.helper.LogicalWorkFlowCreator.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Created by jmgomez on 17/07/14.
 */
public abstract class GenericNotIndexedQueryFilterTest extends GenericConnectorTest {


    private static final String MATCH_ROW = "match_row";
    LogicalWorkFlowCreator logicalWorkFlowCreator;

    @Before
    public void setUp() throws ConnectionException, ExecutionException, InitializationException, UnsupportedException {
        super.setUp();
        logicalWorkFlowCreator = new LogicalWorkFlowCreator(CATALOG, TABLE);
    }


    @Test
    public void selectNotIndexedFilterEqual() throws ExecutionException, UnsupportedException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterEqual ***********************************");

        insertRow(1, 10, 5, clusterNodeName);
        insertRow(2, 9, 1, clusterNodeName);
        insertRow(3, 11, 1, clusterNodeName);
        insertRow(4, 10, 1, clusterNodeName);
        insertRow(5, 20, 1, clusterNodeName);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addEqualFilter(COLUMN_AGE, new Integer(10),false).getLogicalWorkflow();

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);


        assertEquals("The record number is correct", 2, queryResult.getResultSet().size());

        //First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=1;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=1;Column2}"));
        assertTrue("Return correct record", proveSet.contains("money={5}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));

        //Fourth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=4;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=4;Column2}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));

    }


    @Test
    public void selectNotIndexedFilterBetween() throws ExecutionException, UnsupportedException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterBetween ***********************************");

        insertRow(1, 1, 10, clusterNodeName);
        insertRow(2, 1, 9, clusterNodeName);
        insertRow(3, 1, 11, clusterNodeName);
        insertRow(4, 1, 10, clusterNodeName);
        insertRow(5, 1, 20, clusterNodeName);
        insertRow(6, 1, 11, clusterNodeName);
        insertRow(7, 1, 8, clusterNodeName);
        insertRow(8, 1, 12, clusterNodeName);

        refresh(CATALOG);


        //  LogicalWorkflow logicalPlan = createLogicalWorkFlow(BETWEEN_FILTER);
        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addBetweenFilter(COLUMN_AGE, null, null).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 14, proveSet.size());
        assertTrue("Return correct record", proveSet.contains("bin1ValueBin1_r1"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r1"));
        assertTrue("Return correct record", proveSet.contains("bin1ValueBin1_r2"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r2"));
        assertTrue("Return correct record", proveSet.contains("bin1ValueBin1_r3"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r3"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r4"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record", proveSet.contains("bin2ValueBin2_r6"));
        assertTrue("Return correct record", proveSet.contains("money10"));
        assertTrue("Return correct record", proveSet.contains("money9"));
        assertTrue("Return correct record", proveSet.contains("money11"));
        assertTrue("Return correct record", proveSet.contains("age1"));

    }


    @Test
    public void selectNoNotIndexedFilterGreaterEqual() throws ExecutionException, UnsupportedException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNoNotIndexedFilterGreaterEqual ***********************************");


        insertRow(1, 10, 15, clusterNodeName);
        insertRow(2, 9, 10, clusterNodeName);
        insertRow(3, 11, 9, clusterNodeName);
        insertRow(4, 10, 7, clusterNodeName);
        insertRow(5, 7, 9, clusterNodeName);
        insertRow(6, 11, 100, clusterNodeName);
        insertRow(7, 8, 1, clusterNodeName);
        insertRow(8, 12, 10, clusterNodeName);

        refresh(CATALOG);

        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addGreaterEqualFilter(COLUMN_AGE, new Integer("10"),false).getLogicalWorkflow();

        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 5, queryResult.getResultSet().size());

        //First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=1;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=1;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={15}"));
        //Third row
        assertTrue("Return correct record", proveSet.contains("column1={Row=3;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=3;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={9}"));
        //fourth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=4;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=4;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={7}"));

        //sixth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=6;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=6;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={100}"));

        //eigth row
        assertTrue("Return correct record", proveSet.contains("column2={Row=8;Column2}"));
        assertTrue("Return correct record", proveSet.contains("column1={Row=8;Column1}"));
        assertTrue("Return correct record", proveSet.contains("age={12}"));
        assertTrue("Return correct record", proveSet.contains("money={10}"));


    }


    @Test
    public void selectNotIndexedFilterGreater() throws UnsupportedException, ExecutionException {

        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterGreater ***********************************");

        insertRow(1, 10, 1, clusterNodeName);
        insertRow(2, 9, 1, clusterNodeName);
        insertRow(3, 11, 1, clusterNodeName);
        insertRow(4, 10, 1, clusterNodeName);
        insertRow(5, 20, 1, clusterNodeName);
        insertRow(6, 7, 1, clusterNodeName);
        insertRow(7, 8, 1, clusterNodeName);
        insertRow(8, 12, 1, clusterNodeName);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addGreaterFilter(COLUMN_AGE, new Integer("10"),false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 3, queryResult.getResultSet().size());
        //First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=3;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=3;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Fifth row
        assertTrue("Return correct record", proveSet.contains("column2={Row=5;Column2}"));
        assertTrue("Return correct record", proveSet.contains("column1={Row=5;Column1}"));
        assertTrue("Return correct record", proveSet.contains("age={20}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Eigth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=8;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=8;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={12}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));


    }


    @Test
    public void selectNotIndexedFilterLower() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterLower ***********************************");


        insertRow(1, 10, 1, clusterNodeName);
        insertRow(2, 9, 1, clusterNodeName);
        insertRow(3, 11, 1, clusterNodeName);
        insertRow(4, 10, 1, clusterNodeName);
        insertRow(5, 20, 1, clusterNodeName);
        insertRow(6, 7, 1, clusterNodeName);
        insertRow(7, 8, 1, clusterNodeName);
        insertRow(8, 12, 1, clusterNodeName);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addNLowerFilter(COLUMN_AGE, new Integer("10"), false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 3, queryResult.getResultSet().size());

        //Second row
        assertTrue("Return correct record", proveSet.contains("column1={Row=2;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=2;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={9}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Sixth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=6;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=6;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={7}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Seventh row
        assertTrue("Return correct record", proveSet.contains("column1={Row=7;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=7;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={8}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));


    }


    @Test
    public void selectNotIndexedFilterLowerEqual() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNotIndexedFilterLowerEqual ***********************************");


        insertRow(1, 10, 1, clusterNodeName);
        insertRow(2, 9, 1, clusterNodeName);
        insertRow(3, 11, 1, clusterNodeName);
        insertRow(4, 10, 1, clusterNodeName);
        insertRow(5, 20, 1, clusterNodeName);
        insertRow(6, 7, 1, clusterNodeName);
        insertRow(7, 8, 1, clusterNodeName);
        insertRow(8, 12, 1, clusterNodeName);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addLowerEqualFilter(COLUMN_AGE, new Integer("10"),false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 5, queryResult.getResultSet().size());


        //First row
        assertTrue("Return correct record", proveSet.contains("column1={Row=1;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=1;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Second row
        assertTrue("Return correct record", proveSet.contains("column1={Row=2;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=2;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={9}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));
        //fourth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=4;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=4;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={10}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));
        //Sixth row
        assertTrue("Return correct record", proveSet.contains("column1={Row=6;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=6;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={7}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

        //Seventh row
        assertTrue("Return correct record", proveSet.contains("column1={Row=7;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=7;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={8}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));

    }


    @Test
    public void selectNotIndexedFilterDistinct() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNoPKFilterDistinct ***********************************");


        insertRow(1, 10, 1, clusterNodeName);
        insertRow(2, 9, 1, clusterNodeName);
        insertRow(3, 11, 5, clusterNodeName);
        insertRow(4, 10, 1, clusterNodeName);
        insertRow(5, 10, 7, clusterNodeName);

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addDefaultColumns().addDistinctFilter(COLUMN_AGE, new Integer("10"), false).getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);

        Set<Object> proveSet = createCellsResult(queryResult);

        assertEquals("The record number is correct", 2, queryResult.getResultSet().size());

        //Second row
        assertTrue("Return correct record", proveSet.contains("column1={Row=2;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=2;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={9}"));
        assertTrue("Return correct record", proveSet.contains("money={5}"));

        //Third row
        assertTrue("Return correct record", proveSet.contains("column1={Row=3;Column1}"));
        assertTrue("Return correct record", proveSet.contains("column2={Row=3;Column2}"));
        assertTrue("Return correct record", proveSet.contains("age={11}"));
        assertTrue("Return correct record", proveSet.contains("money={1}"));



    }




    @Test
    public void selectNotIndexedFilterMatch() throws UnsupportedException, ExecutionException {
        ClusterName clusterNodeName = getClusterName();
        System.out.println("*********************************** INIT FUNCTIONAL TEST selectNoPKFilterDistinct ***********************************");


        String[] danteParadise = {
        "His glory, by whose might all things are mov'd, Pierces the universe, and in one part Sheds more resplendence, elsewhere less. ",
        " In heav'n, That largeliest of his light partakes, was I, Witness of things, which to relate again Surpasseth power of him who comes from thence;",
        "For that, so near approaching its desire Our intellect is to such depth absorb'd,That memory cannot follow.",
        "That memory cannot follow.  Nathless all, That in my thoughts I of that sacred realm Could store, shall now be matter of my song",
        "Benign Apollo! this last labour aid, And make me such a vessel of thy worth, As thy own laurel claims of me belov'd.",
        "Thus far hath one of steep Parnassus' brows Suffic'd me;",
        "henceforth there is need of both For my remaining enterprise Do thou Enter into my bosom, and there breathe So, as when Marsyas by thy hand was dragg'd Forth from his limbs unsheath'd.",
        "O power divine! If thou to me of shine impart so much, That of that happy realm the shadow'd form Trac'd in my thoughts I may set forth to view, Thou shalt behold me of thy favour'd tree Come to the foot, and crown myself with leaves",
        "For to that honour thou, and my high theme Will fit me.",
        "If but seldom, mighty Sire! To grace his triumph gathers thence a wreath Caesar or bard (more shame for human wills Deprav'd) joy to the Delphic od must spring From the Pierian foliage, when one breast Is with such thirst inspir'd.",
        "From a small spark Great flame hath risen: after me perchance Others with better voice may pray, and gain From the Cirrhaean city answer kind.",
        "Through diver passages, the world's bright lamp Rises to mortals, but through that which joins Four circles with the threefold cross, in best Course, and in happiest constellation set He comes, and to the worldly wax best gives Its temper and impression.",
        "Morning there, Here eve was by almost such passage made;",
        "And whiteness had o'erspread that hemisphere, Blackness the other part;",
        "when to the leftI saw Beatrice turn'd, and on the sun Gazing, as never eagle fix'd his ken.",
        "As from the first a second beam is wont To issue, and reflected upwards rise, E'en as a pilgrim bent on his return, So of her act, that through the eyesight pass'd Into my fancy, mine was form'd; and straight, Beyond our mortal wont, I fix'd mine eyes Upon the sun.",
        "Much is allowed us there, That here exceeds our pow'r; thanks to the place Made for the dwelling of the human kind",
        "I suffer'd it not long, and yet so long That I beheld it bick'ring sparks around, As iron that comes boiling from the fire.",
        "And suddenly upon the day appear'd A day new-ris'n, as he, who hath the power, Had with another sun bedeck'd the sky.",
        "Her eyes fast fix'd on the eternal wheels, Beatrice stood unmov'd; and I with ken Fix'd upon her, from upward gaze remov'd At her aspect, such inwardly became As Glaucus, when he tasted of the herb, That made him peer among the ocean gods;",
        "Words may not tell of that transhuman change:",
        "And therefore let the example serve, though weak, For those whom grace hath better proof in store",
        "If I were only what thou didst create, Then newly, Love! by whom the heav'n is rul'd, Thou know'st, who by thy light didst bear me up.",
        "Whenas the wheel which thou dost ever guide, Desired Spirit! with its harmony Temper'd of thee and measur'd, charm'd mine ear, Then seem'd to me so much of heav'n to blaze With the sun's flame, that rain or flood ne'er made A lake so broad.",
        "The newness of the sound, And that great light, inflam'd me with desire, Keener than e'er was felt, to know their cause.",
        "Whence she who saw me, clearly as myself, To calm my troubled mind, before I ask'd, Open'd her lips, and gracious thus began:",
        "\"With false imagination thou thyself Mak'st dull, so that thou seest not the thing, Which thou hadst seen, had that been shaken off.",
        "Thou art not on the earth as thou believ'st;",
        "For light'ning scap'd from its own proper place Ne'er ran, as thou hast hither now return'd.\"",
        "Although divested of my first-rais'd doubt, By those brief words, accompanied with smiles, Yet in new doubt was I entangled more, And said: ",
        "\"Already satisfied, I rest From admiration deep, but now admire How I above those lighter bodies rise.\"",
        "Whence, after utt'rance of a piteous sigh, She tow'rds me bent her eyes, with such a look, As on her frenzied child a mother casts;",
        "Then thus began: \"Among themselves all things Have order; and from hence the form, which makes The universe resemble God.  ",
        "In this The higher creatures see the printed steps Of that eternal worth, which is the end Whither the line is drawn. ",
        "All natures lean, In this their order, diversely, some more, Some less approaching to their primal source.",
        "Thus they to different havens are mov'd on Through the vast sea of being, and each one With instinct giv'n, that bears it in its course;",
        "This to the lunar sphere directs the fire, This prompts the hearts of mortal animals, This the brute earth together knits, and binds.,",
        "Nor only creatures, void of intellect, Are aim'd at by this bow; but even those, That have intelligence and love, are pierc'd.",
        "That Providence, who so well orders all, With her own light makes ever calm the heaven, In which the substance, that hath greatest speed, Is turn'd: ",
        "and thither now, as to our seat Predestin'd, we are carried by the force Of that strong cord, that never looses dart, But at fair aim and glad.",
        "Yet is it true, That as ofttimes but ill accords the form To the design of art, through sluggishness Of unreplying matter, so this course Is sometimes quitted by the creature, who Hath power, directed thus, to bend elsewhere;,",
        "As from a cloud the fire is seen to fall, From its original impulse warp'd, to earth, By vicious fondness. ",
        "Thou no more admire Thy soaring, (if I rightly deem,) than lapse Of torrent downwards from a mountain's height.",
        "There would in thee for wonder be more cause, If, free of hind'rance, thou hadst fix'd thyself Below, like fire unmoving on the earth.\"",
        "So said, she turn'd toward the heav'n her face. "};

        insertRowMatch(danteParadise,getClusterName());

        refresh(CATALOG);


        LogicalWorkflow logicalPlan = logicalWorkFlowCreator.addColumnName(MATCH_ROW).addMatchFilter(MATCH_ROW, "matter").getLogicalWorkflow();
        QueryResult queryResult = (QueryResult) connector.getQueryEngine().execute(clusterNodeName, logicalPlan);



        ResultSet resultSet = queryResult.getResultSet();
        assertEquals("The record number is correct", 2, resultSet.size());

        String[] result =
                {"That memory cannot follow.  Nathless all, That in my thoughts I of that sacred realm Could store, shall now be matter of my song",
        "Yet is it true, That as ofttimes but ill accords the form To the design of art, through sluggishness Of unreplying matter, so this course Is sometimes quitted by the creature, who Hath power, directed thus, to bend elsewhere;,"};
        int i=0;
        for (Row row :resultSet){
            assertEquals("the return text is correct",result[i++],row.getCell(MATCH_ROW).getValue());
        }


    }


    private Set<Object> createCellsResult(QueryResult queryResult) {
        Set<Object> proveSet = new HashSet<>();
        Iterator<Row> rowIterator = queryResult.getResultSet().iterator();
        while (rowIterator.hasNext()) {
            Row row = rowIterator.next();
            for (String cell : row.getCells().keySet()) {
                proveSet.add(cell + "={" + row.getCell(cell).getValue() + "}");
                System.out.println(cell + "={" + row.getCell(cell).getValue() + "}");
            }
        }
        return proveSet;
    }


    private void insertRow(int ikey, int age, int money, ClusterName clusterNodeName) throws UnsupportedOperationException, ExecutionException, UnsupportedException {

        Row row = new Row();
        Map<String, Cell> cells = new HashMap<>();
        cells.put(COLUMN_1, new Cell("Row=" + ikey + ";Column1"));
        cells.put(COLUMN_2, new Cell("Row=" + ikey + ";Column2"));
        cells.put(COLUMN_3, new Cell("Row=" + ikey + ";Column3"));
        cells.put(COLUMN_AGE, new Cell(age));
        cells.put(COLUMN_MONEY, new Cell(money));
        row.setCells(cells);
        connector.getStorageEngine().insert(clusterNodeName, new TableMetadata(new TableName(CATALOG, TABLE), null, null, null, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST), row);

    }


    private void insertRowMatch(String[] text, ClusterName clusterNodeName) throws UnsupportedOperationException, ExecutionException, UnsupportedException {

        Collection<Row> rows = new ArrayList();
        for (int i=0;i<text.length;i++) {
            Map<String, Cell> cells = new HashMap<>();
            rows.add(new Row(MATCH_ROW,  new Cell(text[i])));
            rows.add(new Row("id",new Cell(i)));
        }

        connector.getStorageEngine().insert(clusterNodeName, new TableMetadata(new TableName(CATALOG, TABLE), null, null, null, null, Collections.EMPTY_LIST, Collections.EMPTY_LIST), rows);

    }


}
