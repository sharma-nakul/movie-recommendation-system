package movie.rdd.functions;

/**
 * Created by Shounak on 5/2/2016.
 */

        import movie.model.MovieGenreRating;
        import org.apache.spark.api.java.function.Function;
        import org.apache.spark.sql.Row;

/**
 * Created by Shounak on on 2nd May 2016
 * To map store 1-1 mapping of genre and movies and ratings
 */
public class MapGenresAvgRatingsUDF implements Function<Row,MovieGenreRating> {
    @Override
    public MovieGenreRating call(Row row) throws Exception{
        try{
            return new MovieGenreRating(row.getString(0),row.getDouble(1),row.getString(2));
        }
        catch(NullPointerException e){
            return new MovieGenreRating(row.getString(0),0.0,row.getString(2));
        }
    }
}