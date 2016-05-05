package movie.rdd.functions;

import movie.model.MovieRecommendation;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * Created by Naks on 02-May-16.
 * Movie Recommendation mapping function
 */
public class MapRecoUDF implements Function<Row,MovieRecommendation> {
    @Override
    public MovieRecommendation call(Row row) throws Exception {
        return new MovieRecommendation(row.getInt(0),row.getString(2),row.getFloat(1));
    }
}
