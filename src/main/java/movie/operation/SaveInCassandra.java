package movie.operation;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import movie.model.*;
import movie.rdd.MapMovieUDF;
import movie.rdd.MapRatingUDF;
import movie.rdd.MapRecoUDF;
import movie.rdd.MapTagUDF;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created by Naks on 02-May-16.
 * Db Operations to save in cassandra
 * CREATE KEYSPACE "movies" WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
 * CREATE TABLE movies.movies_list (movie_id int PRIMARY KEY, movie_name text, genre_list list<text>);
 * CREATE TABLE movies.ratings (user_id int, movie_id int, rating_given_by_user float, PRIMARY KEY(user_id,movie_id));
 * CREATE TABLE movies.tags (user_id int, movie_id int, tag text, PRIMARY KEY(user_id,movie_id,tag));
 *
 */

public class SaveInCassandra {

    private static final Logger logger = LoggerFactory.getLogger(SaveInCassandra.class);

    private JavaRDD<Movie> moviesRDD;
    private JavaRDD<Rating> ratingRDD;
    private JavaRDD<Tag> tagRDD;
    private SQLContext sqlContext;
    private JavaSparkContext jsc;

    public SaveInCassandra(SparkContext sc) {
        this.jsc=JavaSparkContext.fromSparkContext(sc);
        sqlContext=new SQLContext(sc);
        this.moviesRDD = jsc.textFile(CONSTANT.getMoviesFilePath()).map(new MapMovieUDF());
        this.ratingRDD =jsc.textFile(CONSTANT.getRatingsFilePath()).map(new MapRatingUDF());
        this.tagRDD=jsc.textFile(CONSTANT.getTagsFilePath()).map(new MapTagUDF());
        //Creating Cluster object for Cassandra
        Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        //Creating Session object for Cassandra
        Session session = cluster.connect(CONSTANT.getKeySpace());

    }

    public void saveMovies(){
        CassandraJavaUtil.javaFunctions(moviesRDD)
                .writerBuilder(CONSTANT.getKeySpace(), CONSTANT.getMovieListTable(), mapToRow(Movie.class))
                .saveToCassandra();
        logger.info("Movies saved in cassandra successfully");
    }

    public void saveRatings(){
        CassandraJavaUtil.javaFunctions(ratingRDD)
                .writerBuilder(CONSTANT.getKeySpace(), CONSTANT.getRatingsTable(),mapToRow(Rating.class))
                .saveToCassandra();
        logger.info("Ratings saved in cassandra successfully");
    }

    public void saveTags(JavaSparkContext jsc){
        CassandraJavaUtil.javaFunctions(tagRDD)
                .writerBuilder(CONSTANT.getKeySpace(), CONSTANT.getTagsTable(),mapToRow(Tag.class))
                .saveToCassandra();
        logger.info("Tags saved in cassandra successfully");
    }

    public void saveMovieRecommendations(List<PCModel> ratingRecommendation){

        //Convert List<PCModel> to rdd
        JavaRDD<PCModel> recoRDD=jsc.parallelize(ratingRecommendation);

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
        DataFrame joinedDF=recoDF.join(movieDF,recoDF.col("movieId")
                .equalTo(movieDF.col("movieId")))
                .drop(movieDF.col("movieId"));

        JavaRDD<Row> movieRecoDdRDD= joinedDF.toJavaRDD();

        //Convert to RDD to persist in Cassandra
        JavaRDD<MovieRecommendation> movieRecoRDD=movieRecoDdRDD.map(new MapRecoUDF());

        CassandraJavaUtil.javaFunctions(movieRecoRDD)
                .writerBuilder(CONSTANT.getKeySpace(), CONSTANT.getRecoTable(),mapToRow(MovieRecommendation.class))
                .saveToCassandra();
        logger.info("Movie Recommendation saved in cassandra successfully");

    }
}
