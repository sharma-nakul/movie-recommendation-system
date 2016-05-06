package movie.operation;

import movie.model.CONSTANT;
import movie.model.Movie;
import movie.model.Rating;
import movie.model.Tag;
import movie.rdd.functions.MapMovieUDF;
import movie.rdd.functions.MapRatingUDF;
import movie.rdd.functions.MapTagUDF;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


/**
 * Created by Shounak on 5/5/2016.
 * To get the data initially. The plan is to initialize the dataset once and then fetch from cassandra for all later queries
 */
public class GetDataSet {
    private JavaRDD<Movie> moviesRDD;
    private JavaRDD<Rating> ratingRDD;
    private JavaRDD<Tag> tagRDD;
    private JavaSparkContext jsc;


    public GetDataSet(SparkContext sc) {
        this.jsc = JavaSparkContext.fromSparkContext(sc);
        this.moviesRDD = jsc.textFile(CONSTANT.getMoviesFilePath()).map(new MapMovieUDF());
        this.ratingRDD = jsc.textFile(CONSTANT.getRatingsFilePath()).map(new MapRatingUDF());
        this.tagRDD = jsc.textFile(CONSTANT.getTagsFilePath()).map(new MapTagUDF());
        DBService saveInCassandra = new DBService();
        //Added by Shounak on 5th May 2016
        saveInCassandra.saveMovies(this.moviesRDD); // Completed and Working
        saveInCassandra.saveRatings(this.ratingRDD); // Completed and Working
        saveInCassandra.saveTags(this.tagRDD); // Completed and Working
    }
}
