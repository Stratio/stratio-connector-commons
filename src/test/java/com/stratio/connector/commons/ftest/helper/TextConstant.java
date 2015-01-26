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

package com.stratio.connector.commons.ftest.helper;

import java.util.Random;

/**
 * Created by jmgomez on 18/09/14.
 */
public class TextConstant {

        private static Random random = new Random(System.currentTimeMillis());

        public static String[] names = {
                "MARY", "PATRICIA", "LINDA", "BARBARA", "ELIZABETH", "JENNIFER", "MARIA", "SUSAN", "MARGARET",
                "DOROTHY",
                "LISA", "NANCY", "KAREN", "BETTY", "HELEN", "SANDRA", "DONNA", "CAROL", "RUTH", "SHARON", "MICHELLE",
                "LAURA", "SARAH", "KIMBERLY", "DEBORAH", "JESSICA", "SHIRLEY", "CYNTHIA", "ANGELA", "MELISSA", "BRENDA",
                "AMY", "ANNA", "REBECCA", "VIRGINIA", "KATHLEEN", "PAMELA", "MARTHA", "DEBRA", "AMANDA", "STEPHANIE",
                "CAROLYN", "CHRISTINE", "MARIE", "JANET", "CATHERINE", "FRANCES", "ANN", "JOYCE", "DIANE", "ALICE",
                "JULIE",
                "HEATHER", "TERESA", "DORIS", "GLORIA", "EVELYN", "JEAN", "CHERYL", "MILDRED", "KATHERINE", "JOAN",
                "ASHLEY", "JUDITH", "ROSE", "JANICE", "KELLY", "NICOLE", "JUDY", "CHRISTINA", "KATHY", "THERESA",
                "BEVERLY",
                "DENISE", "TAMMY", "IRENE", "JANE", "LORI", "RACHEL", "MARILYN", "ANDREA", "KATHRYN", "LOUISE", "SARA",
                "ANNE", "JACQUELINE", "WANDA", "BONNIE", "JULIA", "RUBY", "LOIS", "TINA", "PHYLLIS", "NORMA", "PAULA",
                "DIANA", "ANNIE", "LILLIAN", "EMILY", "ROBIN", "PEGGY", "CRYSTAL", "GLADYS", "RITA", "DAWN", "CONNIE",
                "FLORENCE", "TRACY", "EDNA", "TIFFANY", "CARMEN", "ROSA", "CINDY", "GRACE", "WENDY", "VICTORIA",
                "EDITH",
                "KIM", "SHERRY", "SYLVIA", "JOSEPHINE", "THELMA", "SHANNON", "SHEILA", "ETHEL", "ELLEN", "ELAINE",
                "MARJORIE", "CARRIE", "CHARLOTTE", "MONICA", "ESTHER", "PAULINE", "EMMA", "JUANITA", "ANITA", "RHONDA",
                "HAZEL", "AMBER", "EVA", "DEBBIE", "APRIL", "LESLIE", "CLARA", "LUCILLE", "JAMIE", "JOANNE", "ELEANOR",
                "VALERIE", "DANIELLE", "MEGAN", "ALICIA", "SUZANNE", "MICHELE", "GAIL", "BERTHA", "DARLENE", "VERONICA",
                "JILL", "ERIN", "GERALDINE", "LAUREN", "CATHY", "JOANN", "LORRAINE", "LYNN", "SALLY", "REGINA", "ERICA",
                "BEATRICE", "DOLORES", "BERNICE", "AUDREY", "YVONNE", "ANNETTE", "JUNE", "SAMANTHA", "MARION", "DANA",
                "STACY", "ANA", "RENEE", "IDA", "VIVIAN", "ROBERTA", "HOLLY", "BRITTANY", "MELANIE", "LORETTA",
                "YOLANDA",
                "JEANETTE", "LAURIE", "KATIE", "KRISTEN", "VANESSA", "ALMA", "SUE", "ELSIE", "BETH", "JEANNE", "VICKI",
                "CARLA", "TARA", "ROSEMARY", "EILEEN", "TERRI", "GERTRUDE", "LUCY", "TONYA", "ELLA", "STACEY", "WILMA",
                "GINA", "KRISTIN", "JESSIE", "NATALIE", "AGNES", "VERA", "WILLIE", "CHARLENE", "BESSIE", "DELORES",
                "MELINDA", "PEARL", "ARLENE", "MAUREEN", "COLLEEN", "ALLISON", "TAMARA", "JOY", "GEORGIA", "CONSTANCE",
                "LILLIE", "CLAUDIA", "JACKIE", "MARCIA", "TANYA", "NELLIE", "MINNIE", "MARLENE", "HEIDI", "GLENDA",
                "LYDIA",
                "VIOLA", "COURTNEY", "MARIAN", "STELLA", "CAROLINE", "DORA", "JO", "VICKIE", "MATTIE", "TERRY",
                "MAXINE",
                "IRMA", "MABEL", "MARSHA", "MYRTLE", "LENA", "CHRISTY", "DEANNA", "PATSY", "HILDA", "GWENDOLYN",
                "JENNIE",
                "JAMES", "JOHN", "ROBERT", "MICHAEL", "WILLIAM", "DAVID", "RICHARD", "CHARLES", "JOSEPH", "THOMAS",
                "CHRISTOPHER", "DANIEL", "PAUL", "MARK", "DONALD", "GEORGE", "KENNETH", "STEVEN", "EDWARD", "BRIAN",
                "RONALD", "ANTHONY", "KEVIN", "JASON", "MATTHEW", "GARY", "TIMOTHY", "JOSE", "LARRY", "JEFFREY",
                "FRANK",
                "SCOTT", "ERIC", "STEPHEN", "ANDREW", "RAYMOND", "GREGORY", "JOSHUA", "JERRY", "DENNIS", "WALTER",
                "PATRICK", "PETER", "HAROLD", "DOUGLAS", "HENRY", "CARL", "ARTHUR", "RYAN", "ROGER", "JOE", "JUAN",
                "JACK",
                "ALBERT", "JONATHAN", "JUSTIN", "TERRY", "GERALD", "KEITH", "SAMUEL", "WILLIE", "RALPH", "LAWRENCE",
                "NICHOLAS", "ROY", "BENJAMIN", "BRUCE", "BRANDON", "ADAM", "HARRY", "FRED", "WAYNE", "BILLY", "STEVE",
                "LOUIS", "JEREMY", "AARON", "RANDY", "HOWARD", "EUGENE", "CARLOS", "RUSSELL", "BOBBY", "VICTOR",
                "MARTIN",
                "ERNEST", "PHILLIP", "TODD", "JESSE", "CRAIG", "ALAN", "SHAWN", "CLARENCE", "SEAN", "PHILIP", "CHRIS",
                "JOHNNY", "EARL", "JIMMY", "ANTONIO", "DANNY", "BRYAN", "TONY", "LUIS", "MIKE", "STANLEY", "LEONARD",
                "NATHAN", "DALE", "MANUEL", "RODNEY", "CURTIS", "NORMAN", "ALLEN", "MARVIN", "VINCENT", "GLENN",
                "JEFFERY",
                "TRAVIS", "JEFF", "CHAD", "JACOB", "LEE", "MELVIN", "ALFRED", "KYLE", "FRANCIS", "BRADLEY", "JESUS",
                "HERBERT", "FREDERICK", "RAY", "JOEL", "EDWIN", "DON", "EDDIE", "RICKY", "TROY", "RANDALL", "BARRY",
                "ALEXANDER", "BERNARD", "MARIO", "LEROY", "FRANCISCO", "MARCUS", "MICHEAL", "THEODORE", "CLIFFORD",
                "MIGUEL", "OSCAR", "JAY", "JIM", "TOM", "CALVIN", "ALEX", "JON", "RONNIE", "BILL", "LLOYD", "TOMMY",
                "LEON",
                "DEREK", "WARREN", "DARRELL", "JEROME", "FLOYD", "LEO", "ALVIN", "TIM", "WESLEY", "GORDON", "DEAN",
                "GREG",
                "JORGE", "DUSTIN", "PEDRO", "DERRICK", "DAN", "LEWIS", "ZACHARY", "COREY", "HERMAN", "MAURICE",
                "VERNON",
                "ROBERTO", "CLYDE", "GLEN", "HECTOR", "SHANE", "RICARDO", "SAM", "RICK", "LESTER", "BRENT", "RAMON",
                "CHARLIE", "TYLER", "GILBERT", "GENE", "MARC", "REGINALD", "RUBEN", "BRETT", "ANGEL", "NATHANIEL",
                "RAFAEL",
                "LESLIE", "EDGAR", "MILTON", "RAUL", "BEN", "CHESTER", "CECIL", "DUANE", "FRANKLIN", "ANDRE", "ELMER",
                "BRAD", "GABRIEL", "RON", "MITCHELL", "ROLAND", "ARNOLD", "HARVEY", "JARED", "ADRIAN", "KARL", "CORY",
                "CLAUDE", "ERIK", "DARRYL", "JAMIE", "NEIL", "JESSIE", "CHRISTIAN", "JAVIER", "FERNANDO", "CLINTON",
                "TED",
                "MATHEW", "TYRONE", "DARREN", "LONNIE", "LANCE", "CODY", "JULIO", "KELLY", "KURT", "ALLAN", "NELSON",
                "GUY",
                "CLAYTON", "HUGH", "MAX", "DWAYNE", "DWIGHT", "ARMANDO", "FELIX", "JIMMIE", "EVERETT", "JORDAN", "IAN",
                "WALLACE", "KEN", "BOB", "JAIME", "CASEY", "ALFREDO", "ALBERTO", "DAVE", "IVAN", "JOHNNIE", "SIDNEY",
                "BYRON", "JULIAN", "ISAAC", "MORRIS", "CLIFTON", "WILLARD", "DARYL", "ROSS", "VIRGIL", "ANDY",
                "MARSHALL",
                "SALVADOR", "PERRY", "KIRK", "SERGIO", "MARION", "TRACY", "SETH", "KENT", "TERRANCE", "RENE", "EDUARDO",
                "TERRENCE", "ENRIQUE", "FREDDIE", "WADE", "AUSTIN", "STUART", "FREDRICK", "ARTURO", "ALEJANDRO",
                "JACKIE",
                "JOEY", "NICK", "LUTHER", "WENDELL", "JEREMIAH", "EVAN", "JULIUS", "DANA", "DONNIE", "OTIS", "SHANNON",
                "TREVOR", "OLIVER", "LUKE", "HOMER", "GERARD", "DOUG", "KENNY", "HUBERT", "ANGELO", "SHAUN", "LYLE",
                "MATT",
                "LYNN", "ALFONSO", "ORLANDO", "REX", "CARLTON", "ERNESTO", "CAMERON", "NEAL", "PABLO", "LORENZO",
                "OMAR",
                "WILBUR", "BLAKE", "GRANT", "HORACE", "RODERICK", "KERRY",
                "ABRAHAM", "WILLIS", "RICKEY", "JEAN", "IRA", "ANDRES", "CESAR", "JOHNATHAN", "MALCOLM", "RUDOLPH",
                "DAMON",
                "KELVIN", "RUDY", "PRESTON", "ALTON", "ARCHIE", "MARCO", "WM", "PETE", "RANDOLPH", "GARRY", "GEOFFREY",
                "JONATHON", "FELIPE", "BENNIE", "GERARDO", "ED", "DOMINIC", "ROBIN", "LOREN", "DELBERT", "COLIN",
                "GUILLERMO", "EARNEST", "LUCAS", "BENNY", "NOEL", "SPENCER", "RODOLFO", "MYRON", "EDMUND", "GARRETT",
                "SALVATORE", "CEDRIC", "LOWELL", "GREGG", "SHERMAN", "WILSON", "DEVIN", "SYLVESTER", "KIM", "ROOSEVELT",
                "ISRAEL", "JERMAINE", "FORREST", "WILBERT", "LELAND", "SIMON", "GUADALUPE", "CLARK", "IRVING",
                "CARROLL",
                "BRYANT", "OWEN", "RUFUS", "WOODROW", "SAMMY", "KRISTOPHER", "MACK", "LEVI", "MARCOS", "GUSTAVO",
                "JAKE",
                "LIONEL", "MARTY", "TAYLOR", "ELLIS", "DALLAS", "GILBERTO", "CLINT", "NICOLAS", "LAURENCE", "ISMAEL",
                "ORVILLE", "DREW", "JODY", "ERVIN", "DEWEY", "AL", "WILFRED", "JOSH", "HUGO", "IGNACIO", "CALEB",
                "TOMAS",
                "SHELDON", "ERICK", "FRANKIE", "STEWART", "DOYLE", "DARREL", "ROGELIO", "TERENCE", "SANTIAGO", "ALONZO",
                "ELIAS", "BERT", "ELBERT", "RAMIRO", "CONRAD", "PAT", "NOAH", "GRADY", "PHIL", "CORNELIUS", "LAMAR",
                "ROLANDO", "CLAY", "PERCY", "DEXTER", "BRADFORD", "MERLE", "DARIN", "AMOS", "TERRELL", "MOSES", "IRVIN",
                "SAUL", "ROMAN", "DARNELL", "RANDAL", "TOMMIE", "TIMMY", "DARRIN", "WINSTON", "BRENDAN", "TOBY", "VAN",
                "ABEL", "DOMINICK", "BOYD", "COURTNEY", "JAN", "EMILIO", "ELIJAH", "CARY", "DOMINGO", "SANTOS",
                "AUBREY",
                "EMMETT", "MARLON", "EMANUEL", "JERALD", "EDMOND", "EMIL", "DEWAYNE", "WILL", "OTTO", "TEDDY",
                "REYNALDO",
                "BRET", "MORGAN", "JESS", "TRENT", "HUMBERTO", "EMMANUEL", "STEPHAN", "LOUIE", "VICENTE", "LAMONT",
                "STACY",
                "GARLAND", "MILES", "MICAH", "EFRAIN", "BILLIE", "LOGAN", "HEATH", "RODGER", "HARLEY", "DEMETRIUS",
                "ETHAN",
                "ELDON", "ROCKY", "PIERRE", "JUNIOR", "FREDDY", "ELI", "BRYCE", "ANTOINE", "ROBBIE", "KENDALL", "ROYCE",
                "STERLING", "MICKEY", "CHASE", "GROVER", "ELTON", "CLEVELAND", "DYLAN", "CHUCK", "DAMIAN", "REUBEN",
                "STAN",
                "AUGUST", "LEONARDO", "JASPER", "RUSSEL", "ERWIN", "BENITO", "HANS", "MONTE", "BLAINE", "ERNIE", "CURT",
                "QUENTIN", "AGUSTIN", "MURRAY", "JAMAL", "DEVON", "ADOLFO", "HARRISON", "TYSON", "BURTON", "BRADY",
                "ELLIOTT", "WILFREDO", "BART", "JARROD", "VANCE", "DENIS", "DAMIEN", "JOAQUIN", "HARLAN", "DESMOND",
                "ELLIOT", "DARWIN", "ASHLEY", "GREGORIO", "BUDDY", "XAVIER", "KERMIT", "ROSCOE", "ESTEBAN", "ANTON",
                "SOLOMON", "SCOTTY", "NORBERT", "ELVIN", "WILLIAMS", "NOLAN", "CAREY", "ROD", "QUINTON", "HAL", "BRAIN",
                "ROB", "ELWOOD", "KENDRICK", "DARIUS", "MOISES", "SON", "MARLIN", "FIDEL", "THADDEUS", "CLIFF",
                "MARCEL",
                "ALI", "JACKSON", "RAPHAEL", "BRYON", "ARMAND", "ALVARO", "JEFFRY", "DANE", "JOESPH", "THURMAN", "NED",
                "SAMMIE", "RUSTY", "MICHEL", "MONTY", "RORY", "FABIAN", "REGGIE", "MASON", "GRAHAM", "KRIS", "ISAIAH",
                "VAUGHN", "GUS", "AVERY", "LOYD", "DIEGO", "ALEXIS", "ADOLPH", "NORRIS", "MILLARD", "ROCCO", "GONZALO",
                "DERICK", "RODRIGO", "GERRY", "STACEY", "CARMEN", "WILEY", "RIGOBERTO", "ALPHONSO", "TY", "SHELBY",
                "RICKIE", "NOE", "VERN", "BOBBIE", "REED", "JEFFERSON", "ELVIS", "BERNARDO", "MAURICIO", "HIRAM",
                "DONOVAN",
                "BASIL", "RILEY", "OLLIE", "NICKOLAS", "MAYNARD", "SCOT", "VINCE", "QUINCY", "EDDY", "SEBASTIAN",
                "FEDERICO", "ULYSSES", "HERIBERTO", "DONNELL", "COLE", "DENNY",
                "DAVIS", "GAVIN", "EMERY", "WARD", "ROMEO", "JAYSON", "DION", "DANTE", "CLEMENT", "COY", "ODELL",
                "MAXWELL",
                "JARVIS", "BRUNO", "ISSAC", "MARY", "DUDLEY", "BROCK", "SANFORD", "COLBY", "CARMELO", "BARNEY",
                "NESTOR",
                "HOLLIS", "STEFAN", "DONNY", "ART", "LINWOOD", "BEAU", "WELDON", "GALEN", "ISIDRO", "TRUMAN", "DELMAR",
                "JOHNATHON", "SILAS", "FREDERIC", "DICK", "KIRBY", "IRWIN", "CRUZ", "MERLIN", "MERRILL", "CHARLEY",
                "MARCELINO", "LANE", "HARRIS", "CLEO", "CARLO", "TRENTON", "KURTIS", "HUNTER", "AURELIO", "WINFRED",
                "VITO",
                "COLLIN", "DENVER", "CARTER", "LEONEL", "EMORY", "PASQUALE", "MOHAMMAD", "MARIANO", "DANIAL", "BLAIR",
                "LANDON", "DIRK", "BRANDEN", "ADAN", "NUMBERS", "CLAIR", "BUFORD", "GERMAN", "BERNIE", "WILMER", "JOAN",
                "EMERSON", "ZACHERY", "FLETCHER", "JACQUES", "ERROL", "DALTON", "MONROE", "JOSUE", "DOMINIQUE",
                "EDWARDO",
                "BOOKER", "WILFORD", "SONNY", "SHELTON", "CARSON", "THERON", "RAYMUNDO", "DAREN", "TRISTAN", "HOUSTON",
                "ROBBY", "LINCOLN", "JAME", "GENARO", "GALE", "BENNETT", "OCTAVIO", "CORNELL", "LAVERNE", "HUNG",
                "ARRON",
                "ANTONY", "HERSCHEL", "ALVA", "GIOVANNI", "GARTH", "CYRUS", "CYRIL", "RONNY", "STEVIE", "LON",
                "FREEMAN",
                "ERIN", "DUNCAN", "KENNITH", "CARMINE", "AUGUSTINE", "YOUNG", "ERICH", "CHADWICK", "WILBURN", "RUSS",
                "REID", "MYLES", "ANDERSON", "MORTON", "JONAS", "FOREST", "MITCHEL", "MERVIN", "ZANE", "RICH", "JAMEL",
                "LAZARO", "ALPHONSE", "RANDELL", "MAJOR", "JOHNIE", "JARRETT", "BROOKS", "ARIEL", "ABDUL", "DUSTY",
                "LUCIANO", "LINDSEY", "TRACEY", "SEYMOUR", "SCOTTIE", "EUGENIO", "MOHAMMED", "SANDY", "VALENTIN",
                "CHANCE",
                "ARNULFO", "LUCIEN", "FERDINAND", "THAD", "EZRA", "SYDNEY", "ALDO", "RUBIN", "ROYAL", "MITCH", "EARLE",
                "ABE", "WYATT", "MARQUIS", "LANNY", "KAREEM", "JAMAR", "BORIS", "ISIAH", "EMILE", "ELMO", "ARON",
                "LEOPOLDO", "EVERETTE", "JOSEF", "GAIL", "ELOY", "DORIAN", "RODRICK", "REINALDO", "LUCIO", "JERROD",
                "WESTON", "HERSHEL", "BARTON", "PARKER", "LEMUEL", "LAVERN", "BURT", "JULES", "GIL", "ELISEO", "AHMAD",
                "NIGEL", "EFREN", "ANTWAN", "ALDEN", "MARGARITO", "COLEMAN", "REFUGIO", "DINO", "OSVALDO", "LES",
                "DEANDRE",
                "NORMAND", "KIETH", "IVORY", "ANDREA", "TREY", "NORBERTO", "NAPOLEON", "JEROLD", "FRITZ", "ROSENDO",
                "MILFORD", "SANG", "DEON", "CHRISTOPER", "ALFONZO", "LYMAN", "JOSIAH", "BRANT", "WILTON", "RICO",
                "JAMAAL",
                "DEWITT", "CAROL", "BRENTON", "YONG", "OLIN", "FOSTER", "FAUSTINO", "CLAUDIO", "JUDSON", "GINO",
                "EDGARDO",
                "BERRY", "ALEC", "TANNER", "JARRED", "DONN", "TRINIDAD", "TAD", "SHIRLEY", "PRINCE", "PORFIRIO", "ODIS",
                "MARIA", "LENARD", "CHAUNCEY", "CHANG", "TOD", "MEL", "MARCELO", "KORY", "AUGUSTUS", "KEVEN", "HILARIO",
                "BUD", "SAL", "ROSARIO", "ORVAL", "MAURO", "DANNIE", "ZACHARIAH", "OLEN", "ANIBAL", "MILO", "JED",
                "FRANCES", "THANH", "DILLON", "AMADO", "NEWTON", "CONNIE" };

        public static String[] danteParadise = {
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
                "So said, she turn'd toward the heav'n her face. " };

        public static String[] countries = { "Afghanistan",
                "Albania",
                "Algeria",
                "Andorra",
                "Angola",
                "Antigua & Deps",
                "Argentina",
                "Armenia",
                "Australia",
                "Austria",
                "Azerbaijan",
                "Bahamas",
                "Bahrain",
                "Bangladesh",
                "Barbados",
                "Belarus",
                "Belgium",
                "Belize",
                "Benin",
                "Bhutan",
                "Bolivia",
                "Bosnia Herzegovina",
                "Botswana",
                "Brazil",
                "Brunei",
                "Bulgaria",
                "Burkina",
                "Burundi",
                "Cambodia",
                "Cameroon",
                "Canada",
                "Cape Verde",
                "Central African Rep",
                "Chad",
                "Chile",
                "China",
                "Colombia",
                "Comoros",
                "Congo",
                "Congo {Democratic Rep}",
                "Costa Rica",
                "Croatia",
                "Cuba",
                "Cyprus",
                "Czech Republic",
                "Denmark",
                "Djibouti",
                "Dominica",
                "Dominican Republic",
                "East Timor",
                "Ecuador",
                "Egypt",
                "El Salvador",
                "Equatorial Guinea",
                "Eritrea",
                "Estonia",
                "Ethiopia",
                "Fiji",
                "Finland",
                "France",
                "Gabon",
                "Gambia",
                "Georgia",
                "Germany",
                "Ghana",
                "Greece",
                "Grenada",
                "Guatemala",
                "Guinea",
                "Guinea-Bissau",
                "Guyana",
                "Haiti",
                "Honduras",
                "Hungary",
                "Iceland",
                "India",
                "Indonesia",
                "Iran",
                "Iraq",
                "Ireland {Republic}",
                "Israel",
                "Italy",
                "Ivory Coast",
                "Jamaica",
                "Japan",
                "Jordan",
                "Kazakhstan",
                "Kenya",
                "Kiribati",
                "Korea North",
                "Korea South",
                "Kosovo",
                "Kuwait",
                "Kyrgyzstan",
                "Laos",
                "Latvia",
                "Lebanon",
                "Lesotho",
                "Liberia",
                "Libya",
                "Liechtenstein",
                "Lithuania",
                "Luxembourg",
                "Macedonia",
                "Madagascar",
                "Malawi",
                "Malaysia",
                "Maldives",
                "Mali",
                "Malta",
                "Marshall Islands",
                "Mauritania",
                "Mauritius",
                "Mexico",
                "Micronesia",
                "Moldova",
                "Monaco",
                "Mongolia",
                "Montenegro",
                "Morocco",
                "Mozambique",
                "Myanmar, {Burma}",
                "Namibia",
                "Nauru",
                "Nepal",
                "Netherlands",
                "New Zealand",
                "Nicaragua",
                "Niger",
                "Nigeria",
                "Norway",
                "Oman",
                "Pakistan",
                "Palau",
                "Panama",
                "Papua New Guinea",
                "Paraguay",
                "Peru",
                "Philippines",
                "Poland",
                "Portugal",
                "Qatar",
                "Romania",
                "Russian Federation",
                "Rwanda",
                "St Kitts & Nevis",
                "St Lucia",
                "Saint Vincent & the Grenadines",
                "Samoa",
                "San Marino",
                "Sao Tome & Principe",
                "Saudi Arabia",
                "Senegal",
                "Serbia",
                "Seychelles",
                "Sierra Leone",
                "Singapore",
                "Slovakia",
                "Slovenia",
                "Solomon Islands",
                "Somalia",
                "South Africa",
                "South Sudan",
                "Spain",
                "Sri Lanka",
                "Sudan",
                "Suriname",
                "Swaziland",
                "Sweden",
                "Switzerland",
                "Syria",
                "Taiwan",
                "Tajikistan",
                "Tanzania",
                "Thailand",
                "Togo",
                "Tonga",
                "Trinidad & Tobago",
                "Tunisia",
                "Turkey",
                "Turkmenistan",
                "Tuvalu",
                "Uganda",
                "Ukraine",
                "United Arab Emirates",
                "United Kingdom",
                "United States",
                "Uruguay",
                "Uzbekistan",
                "Vanuatu",
                "Vatican City",
                "Venezuela",
                "Vietnam",
                "Yemen",
                "Zambia",
                "Zimbabwe" };

        public static final String[] TEXT = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O",
                "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "a", "b", "c", "d", "e", "f", "g", "h", "i",
                "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z", "!", "_", "1",
                "2", "3", "4", "5", "6", "7", "8", "9", " " };

        public static String getRandomName() {
                return names[Math.abs(random.nextInt(names.length))];
        }

        public static String getRandomCountry() {
                return countries[Math.abs(random.nextInt(countries.length))];
        }

        public static String getRandomDanteLine() {
                return danteParadise[Math.abs(random.nextInt(danteParadise.length))];
        }

        public static String getRandomText(Integer size) {
                StringBuilder sb = new StringBuilder("");

                for (int i = 0; i < size; i++) {
                        sb.append(TEXT[Math.abs(random.nextInt(TEXT.length))]);
                }

                String retunValue = sb.toString();
                if (retunValue.trim().isEmpty()) {
                        retunValue = getRandomText(size);
                }
                return retunValue;
        }
}

