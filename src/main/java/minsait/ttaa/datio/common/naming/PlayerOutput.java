package minsait.ttaa.datio.common.naming;

import java.util.List;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.expressions.Window;
import static org.apache.spark.sql.functions.*;
import static minsait.ttaa.datio.common.Common.*;

public final class PlayerOutput {

    public static Field catHeightByPosition = new Field(CAT_HEIGHT_BY_POSITION);

    private static final Column[] columns = {
        new Column(COLUMN_SHORT_NAME),
        new Column(COLUMN_LONG_NAME),
        new Column(COLUMN_AGE),
        new Column(COLUMN_HEIGHT_CM),
        new Column(COLUMN_WEIGHT_KG),
        new Column(COLUMN_NATIONALITY),
        new Column(COLUMN_CLUB_NAME),
        new Column(COLUMN_OVERALL),
        new Column(COLUMN_POTENTIAL),
        new Column(COLUMN_TEAM_POSITION),};

    public static Dataset<Row> selectColumns(Dataset<Row> data) {
        return data.select(columns);
    }

    /**
     *description: Método de asignación de rango de edades
     * @param data
     * @return
     */
    public static Dataset<Row> setAgeRange(Dataset<Row> data) {
        return data
                .withColumn(COLUMN_AGE_RANGE,
                        when(col(COLUMN_AGE).$less(23), LETTER_UPPER_CASE_A)
                                .when(col(COLUMN_AGE).$less(27), LETTER_UPPER_CASE_B)
                                .when(col(COLUMN_AGE).$less(32), LETTER_UPPER_CASE_C)
                                .when(col(COLUMN_AGE ).$greater$eq(32), LETTER_UPPER_CASE_D)
                );
    }

    /**
     * description: particiona los datos por la nacionalidad y hace una ordenacion por OVERALL
     * @param data
     * @return
     */
    public static Dataset<Row> partitionNationTeam(Dataset<Row> data) {
        WindowSpec w = Window
                .partitionBy(col(COLUMN_NATIONALITY), col(COLUMN_TEAM_POSITION))
                .orderBy(col(COLUMN_OVERALL).desc());
        return data.withColumn(COLUMN_RANK_NATIONALITY_POSITION, row_number().over(w));
    }

    /**
     * description: obtiene el resultado en una nueva columna dividiendo las columnas POTENTIAL y OVERALL
     * @param data
     * @return
     */
    public static Dataset<Row> potentialVsOverall(Dataset<Row> data) {
        return data.withColumn(COLUMN_POTENTIAL_VS_OVERALL, (col(COLUMN_POTENTIAL).divide(col(COLUMN_OVERALL))));
    }

    /**
     * description: filtra por RANK_NATIONALITY_POSITION y menor a 3
     * @param data
     * @return
     */
    public static Dataset<Row> filterRankAll(Dataset<Row> data) {
        return data.where(col(COLUMN_RANK_NATIONALITY_POSITION).$less(3));
    }

    /**
     * description: filtra por rango de edad que se encuentren en B y C y la columna POTENTIAL_VS_OVERALL sea mayor a 1.15
     * @param data
     * @return
     */
    public static Dataset<Row> filterAgeRangeByBC(Dataset<Row> data) {
        return data.where(col(COLUMN_AGE_RANGE).isin(LETTER_UPPER_CASE_B, LETTER_UPPER_CASE_C).and(col(COLUMN_POTENTIAL_VS_OVERALL).$greater(1.15)));
    }

    /**
     * description: filtra por rango de edad que se encuentren en A y la columna POTENTIAL_VS_OVERALL sea mayor a 1.25
     * @param data
     * @return
     */
    public static Dataset<Row> filterAgeRangeByA(Dataset<Row> data) {
        return data.where(col(COLUMN_AGE_RANGE).equalTo(LETTER_UPPER_CASE_A).and(col(COLUMN_POTENTIAL_VS_OVERALL).$greater(1.25)));
    }

    /**
     * description: filtra por rango de edad que se encuentren en D y la columna RANK_NATIONALITY_POSITION sea menor a 5
     * @param data
     * @return
     */
    public static Dataset<Row> filterAgeRangeByD(Dataset<Row> data) {
        return data.where(col(COLUMN_AGE_RANGE).equalTo(LETTER_UPPER_CASE_D).and(col(COLUMN_RANK_NATIONALITY_POSITION).$less(5)));
    }

    /**
     * description: union de dataset's que contengan las mismas columnas
     * @param data
     * @return
     */
    public static Dataset<Row> unionFilters(List<Dataset<Row>> data) {
        Dataset<Row> dataset = null;
        for (Dataset<Row> item : data) {
            if (dataset == null) {
                dataset = item;
            } else {
                dataset = dataset.union(item);
            }
        }
        return dataset;
    }
}
