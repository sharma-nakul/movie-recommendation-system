package movie.rdd;

import movie.model.PCModel;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by Naks on 02-May-16.
 * Function to create Key-Value Pair of <movieId-recoValue>
 */
public class MovieAndRecoPairFunction implements PairFunction<PCModel,Integer,Float>{

    public Tuple2<Integer,Float> call(PCModel s){

        return new Tuple2<Integer,Float>(s.getMovieId(),s.getRating());
    }
}
