package movie.rdd;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

/**
 * Created by Naks on 02-May-16.
 * Function to create Key-Value Pair of <movieId-movieName>
 */
public class MovieAndNamePairFunction implements PairFunction<Row, Integer, String> {

    public Tuple2<Integer, String> call(Row row) {
        return new Tuple2<Integer, String>(row.getInt(0), row.getString(1));
    }
}
