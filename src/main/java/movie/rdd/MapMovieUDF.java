package movie.rdd;

import movie.model.Movie;
import org.apache.spark.api.java.function.Function;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Created by Naks on 02-May-16.
 * Map the CSV file data with JavaRDD structure
 */
public class MapMovieUDF implements Function<String, Movie> {

    @Override
    public Movie call(String line) throws Exception {
        String[] m = line.split(",");
        Movie movie = new Movie();
        movie.setMovieId(Integer.parseInt(m[0]));
        movie.setMovieName(m[1]);

        ArrayList<String> tempGenre = new ArrayList<String>();
        String[] g = m[2].split("\\|");
        Collections.addAll(tempGenre, g);
        movie.setGenreList(tempGenre);
        return movie;
    }
}


