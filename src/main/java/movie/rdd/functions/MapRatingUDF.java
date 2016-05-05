package movie.rdd.functions;

import movie.model.Rating;
import org.apache.spark.api.java.function.Function;

/**
 * Created by Naks on 02-May-16.
 * Map the CSV file data with JavaRDD structure
 */
public class MapRatingUDF implements Function<String,Rating> {

    @Override
    public Rating call(String line) throws Exception{
        String[] r=line.split(",");
        Rating rating = new Rating();
        rating.setUserId(Integer.parseInt(r[0]));
        rating.setMovieId(Integer.parseInt(r[1]));
        rating.setRatingGivenByUser(Float.parseFloat(r[2]));
        return rating;
    }
}
