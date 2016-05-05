package movie.operation;

import movie.model.*;
import movie.rdd.functions.*;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.count;

/**
 * Created by Naks on 02-May-16.
 * Db Operations to save in cassandra
 * CREATE KEYSPACE "movies" WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
 * CREATE TABLE movies.movies_list (movie_id int PRIMARY KEY, movie_name text, genre_list list<text>);
 * CREATE TABLE movies.ratings (user_id int, movie_id int, rating_given_by_user float, PRIMARY KEY(user_id,movie_id));
 * CREATE TABLE movies.tags (user_id int, movie_id int, tag text, PRIMARY KEY(user_id,movie_id,tag));
 * CREATE TABLE movies.recommendation (movie_id int PRIMARY KEY, movie_name text,reco_value float);
 */

public class RecoMining {

    private static final Logger logger = LoggerFactory.getLogger(RecoMining.class);

    private JavaRDD<Movie> moviesRDD;
    private JavaRDD<Rating> ratingRDD;
    private JavaRDD<Tag> tagRDD;
    private SQLContext sqlContext;
    private JavaSparkContext jsc;
    private DBService dbService;


    public RecoMining(SparkContext sc) {
        this.jsc = JavaSparkContext.fromSparkContext(sc);
        sqlContext = new SQLContext(sc);
        this.moviesRDD = jsc.textFile(CONSTANT.getMoviesFilePath()).map(new MapMovieUDF());
        this.ratingRDD = jsc.textFile(CONSTANT.getRatingsFilePath()).map(new MapRatingUDF());
        this.tagRDD = jsc.textFile(CONSTANT.getTagsFilePath()).map(new MapTagUDF());
        this.dbService = new DBService();
    }

    public void mapMovieAndRecommendations(List<PCModel> ratingRecommendation) {
        //Convert List<PCModel> to rdd
        JavaRDD<PCModel> recoRDD = jsc.parallelize(ratingRecommendation);

        //Register Recommendations RDD into DataFrame -> Recommendation DataFrame
        DataFrame recoDF = sqlContext.createDataFrame(recoRDD, PCModel.class);
        recoDF.registerTempTable("movieRecommendation");

        //Register Movie RDD into DataFrame -> Movie DataFrame
        DataFrame schemaMovieDF = sqlContext.createDataFrame(moviesRDD, Movie.class);
        schemaMovieDF.registerTempTable("movieIdAndName");

        /*--------------------Operations--------------------------------------------*/

        //Pick only two columns (movie_id & movie_name) from Movie DataFrame
        DataFrame movieDF = sqlContext.sql("SELECT movieId, movieName FROM movieIdAndName");

        // Join two DataFrames to form columns -> movieId, rating, movieName
        DataFrame joinedDF = recoDF.join(movieDF, recoDF.col("movieId")
                .equalTo(movieDF.col("movieId")))
                .drop(movieDF.col("movieId"));

        JavaRDD<Row> movieRecoDdRDD = joinedDF.toJavaRDD();

        //Convert to RDD to persist in Cassandra
        JavaRDD<MovieRecommendation> movieRecoRDD = movieRecoDdRDD.map(new MapRecoUDF());

        dbService.saveRecommendation(movieRecoRDD);
    }

    public void BayesianAverageCalculation() {
        /**
         * Steps
         * 1. Average Rating of the movie -> R (averageRating)
         * 2. Number of votes for the movie -> v (count)
         * 3. Minimum votes required to be listed =5 -> m
         * 4. Mean of all movie ratings -> C
         * (v/v+m)*R+(m/v+m)*c
         */

        // Fetch Movie Ratings from table
        DataFrame ratingDF = sqlContext.createDataFrame(ratingRDD, Rating.class);
        ratingDF.registerTempTable("movie_ratings");

        //Fetch MovieId and MovieName from table
        DataFrame schemaMovieDF = sqlContext.createDataFrame(moviesRDD, Movie.class);
        schemaMovieDF.registerTempTable("movieIdAndName");
        DataFrame movieDF = sqlContext.sql("SELECT movieId, movieName FROM movieIdAndName");

        //Calculate Bayesian Average
        double meanOfAllMovieRatings = ratingDF.agg(avg("ratingGivenByUser")).alias("meanOfMovieRating").head().getDouble(0);
        DataFrame tDF = ratingDF.groupBy("movieId").agg(avg("ratingGivenByUser").alias("averageRating"),
                count("movieId").alias("count")).orderBy("count");
        tDF.registerTempTable("tDF");

        String sqlQuery = "select movieId, averageRating, count, ((count/(count+" + CONSTANT.getMinimumVotesRequired() +
                "))*averageRating) + ((" + CONSTANT.getMinimumVotesRequired() + "/CAST((count+" + CONSTANT.getMinimumVotesRequired() +
                ") AS Decimal))*" + meanOfAllMovieRatings + ") AS bayesianAverage from tDF";
        DataFrame bayesianAvgDF = sqlContext.sql(sqlQuery);
        bayesianAvgDF.registerTempTable("bayesianAvgDF");

        /*DataFrame bayesianDF = movieDF.join(bayesianAvgDF, movieDF.col("movieId").equalTo(bayesianAvgDF.col("movieId")))
                .orderBy(desc("bayesianAverage"))
                .drop(bayesianAvgDF.col("movieId"));*/

        //Join MovieId, MovieName and BayesianAverage
        DataFrame bayesianDF = sqlContext.sql("select CAST(movieIdAndName.movieId as Integer), " +
                "movieIdAndName.movieName, averageRating, CAST(count as Integer), " +
                "bayesianAverage from movieIdAndName inner join bayesianAvgDF on movieIdAndName.movieId = bayesianAvgDF.movieId " +
                "order by bayesianAverage desc");

        //Persist results in cassandra
        JavaRDD<Row> bayesianAverageRDD = bayesianDF.toJavaRDD();
        JavaRDD<BayesianAverage> bayesianRDD = bayesianAverageRDD.map(new MapBayesianUDF());
        dbService.saveBayesianAverage(bayesianRDD);

    }
}
