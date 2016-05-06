package movie.rdd.functions;

import movie.model.MovieGenres;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * Created by Shounak on on 2nd May 2016
 * To map store 1-1 mapping of genre and movies
 */
public class MapGenresUDF implements Function<Row,MovieGenres> {
        @Override
        public MovieGenres call(Row row) throws Exception{
            return new MovieGenres(row.getInt(0),row.getString(1),row.getString(2));
        }
}
