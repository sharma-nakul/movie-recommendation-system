package movie.recommendation;

import movie.model.PCModel;
import movie.operation.PearsonCorrelation;
import movie.operation.SaveInCassandra;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.util.List;

import static org.apache.spark.api.java.JavaSparkContext.fromSparkContext;


public class SparkApp {
    private static final String moviesFilePath="spark-streaming\\src\\main\\resources\\movies.csv";
    private static final String ratingsFilePath="spark-streaming\\src\\main\\resources\\ratings.csv";

    private static final int userId=1;
    private static final String keySpace="movies";
    private static final String movieListTable="movies_list";
    private static final String userRatingTable="user_rating";

    public static void main(String[] args) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("MovieRecommendation")
                .setMaster("local[4]")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.cassandra.connection.host", "127.0.0.1");

        SparkContext sc=new SparkContext(sparkConf);
        JavaSparkContext jsc = fromSparkContext(sc);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        SaveInCassandra saveInCassandra= new SaveInCassandra(sc);
        //saveInCassandra.saveMovies(); // Completed and Working
        //saveInCassandra.saveRatings(); // Completed and Working
        //saveInCassandra.saveTags(); // Completed and Working

        PearsonCorrelation pc=new PearsonCorrelation();
        List<PCModel> recommendationBasedOnUserRatings=pc.applyOnRatings();
        saveInCassandra.saveMovieRecommendations(recommendationBasedOnUserRatings);


    }
}