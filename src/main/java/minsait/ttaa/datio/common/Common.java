package minsait.ttaa.datio.common;

import java.util.ResourceBundle;

public final class Common {

    private static final ResourceBundle CONF = ResourceBundle.getBundle("minsait.ttaa.datio.resources.configuration");

    public static final String SPARK_MODE = CONF.getString("spark.mode");
    public static final String HEADER = CONF.getString("header");
    public static final String INFER_SCHEMA = CONF.getString("infer.schema");
    public static final String INPUT_PATH = CONF.getString("input.path");
    public static final String OUTPUT_PATH = CONF.getString("output.path");
    public static final String CAT_HEIGHT_BY_POSITION = CONF.getString("cat.height.position");

    public static final String EJERCICIO_1 = CONF.getString("path.practice.1");
    public static final String EJERCICIO_2 = CONF.getString("path.practice.2");
    public static final String EJERCICIO_3 = CONF.getString("path.practice.3");
    public static final String EJERCICIO_4 = CONF.getString("path.practice.4");
    public static final String EJERCICIO_5 = CONF.getString("path.practice.5");

    public static final String COLUMN_SHORT_NAME = CONF.getString("short.name");
    public static final String COLUMN_LONG_NAME = CONF.getString("long.name");
    public static final String COLUMN_AGE = CONF.getString("age");
    public static final String COLUMN_HEIGHT_CM = CONF.getString("height.cm");
    public static final String COLUMN_WEIGHT_KG = CONF.getString("weight.kg");
    public static final String COLUMN_NATIONALITY = CONF.getString("nationality");
    public static final String COLUMN_CLUB_NAME = CONF.getString("club.name");
    public static final String COLUMN_OVERALL = CONF.getString("overall");
    public static final String COLUMN_POTENTIAL = CONF.getString("potential");
    public static final String COLUMN_TEAM_POSITION = CONF.getString("team.position");
    
    public static final String COLUMN_AGE_RANGE = CONF.getString("team.position");
    public static final String COLUMN_RANK_NATIONALITY_POSITION = CONF.getString("rank.nationality.position");
    public static final String COLUMN_POTENTIAL_VS_OVERALL = CONF.getString("potential.vs.overall");
    
    public static final String LETTER_UPPER_CASE_A = CONF.getString("a.upper.case");
    public static final String LETTER_UPPER_CASE_B = CONF.getString("b.upper.case");
    public static final String LETTER_UPPER_CASE_C = CONF.getString("c.upper.case");
    public static final String LETTER_UPPER_CASE_D = CONF.getString("d.upper.case");

    public static final String PATH_HADOPP = CONF.getString("path.hadoop");
    public static final String HADOOP_HOME_DIR = CONF.getString("hadoop.home.dir");
}
