package movie.rdd.functions;

import movie.model.BayesianAverage;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * Created by Naks on 02-May-16.
 * Bayesian Average Mapping Function
 */
public class MapBayesianUDF implements Function<Row,BayesianAverage> {
    @Override
    public BayesianAverage call(Row row) throws Exception{
        return new BayesianAverage(row.getInt(0),row.getString(1),row.getDouble(2),row.getInt(3),row.getDouble(4));
    }
}
