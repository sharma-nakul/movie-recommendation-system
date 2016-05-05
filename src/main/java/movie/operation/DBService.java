package movie.operation;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import movie.model.*;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * Created by Naks on 04-May-16.
 * DBService interface implementation
 */

public class DBService{

    private static final Logger logger = LoggerFactory.getLogger(DBService.class);

    public void saveMovies(JavaRDD<Movie> moviesRDD) {
        CassandraJavaUtil.javaFunctions(moviesRDD)
                .writerBuilder(CONSTANT.getKeySpace(), CONSTANT.getMovieListTable(), mapToRow(Movie.class))
                .saveToCassandra();
        logger.info("Movies saved in cassandra successfully");

    }

    public void saveRatings(JavaRDD<Rating> ratingsRDD) {
        CassandraJavaUtil.javaFunctions(ratingsRDD)
                .writerBuilder(CONSTANT.getKeySpace(), CONSTANT.getRatingsTable(),mapToRow(Rating.class))
                .saveToCassandra();
        logger.info("Ratings saved in cassandra successfully");
    }

    public void saveTags(JavaRDD<Tag> tagsRDD){
        CassandraJavaUtil.javaFunctions(tagsRDD)
                .writerBuilder(CONSTANT.getKeySpace(), CONSTANT.getTagsTable(),mapToRow(Tag.class))
                .saveToCassandra();
        logger.info("Tags saved in cassandra successfully");
    }

    public void saveRecommendation(JavaRDD<MovieRecommendation> movieRecoRDD){
        CassandraJavaUtil.javaFunctions(movieRecoRDD)
                .writerBuilder(CONSTANT.getKeySpace(), CONSTANT.getRecoTable(),mapToRow(MovieRecommendation.class))
                .saveToCassandra();
        logger.info("Movie Recommendation saved in cassandra successfully");
    }
}
