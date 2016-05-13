package movie.service;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import movie.model.*;
import movie.operation.PearsonCorrelation;
import movie.operation.RecoMining;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Naks on 02-May-16.
 * Implementation of user service
 */

@Service
public class UserServiceImpl implements UserService {

    @Autowired
    Session session;

    @Autowired
    SparkContext sparkContext;

    @Override
    public User getUserStatus(String userId) {

        User user = new User();

        final ResultSet rows = session.execute("SELECT * from " + CONSTANT.getUserInfoTable());

        for (Row row : rows.all()) {
            user.setUserId(row.getInt("user_id"));
            user.setMovieCount(row.getInt("movie_count"));
            if (user.getUserId() == Integer.valueOf(userId)) {
                user.setStatus("success");
                break;
            }
        }
        return user;
    }

    @Override
    public List<TypeParser> generateRecommendation(String type, int userId) {
        List<TypeParser> listTypeParser;
        TypeParser userTypeParser;

        String queryBayesian = "select * from "+CONSTANT.getBayesianTable() +" limit 30";
        String queryPearson="select * from "+CONSTANT.getRecoTable()+" limit 30";

        RecoMining recoMining = new RecoMining(sparkContext);

        if (type.equals("BA")) {
            final ResultSet readBayesianAvgTable = session.execute(queryBayesian);
            listTypeParser= new ArrayList<>();

            for (Row row : readBayesianAvgTable.all()) {
                userTypeParser = new TypeParser();
                userTypeParser.setType(type);
                userTypeParser.setRating(row.getDouble("bayesian_average"));
                userTypeParser.setMovieName(row.getString("movie_name"));
                listTypeParser.add(userTypeParser);
            }
        } else if(type.equals("PC")){
            PearsonCorrelation pc = new PearsonCorrelation();
            List<PCModel> recommendationBasedOnUserRatings = pc.applyOnRatings(userId,
                    CONSTANT.getTopRowFromRecoTable(), CONSTANT.getAccuracyThreshold());

            recoMining.mapMovieAndRecommendations(recommendationBasedOnUserRatings);

            final ResultSet readRecommendationTable=session.execute(queryPearson);
            listTypeParser= new ArrayList<>();

            System.out.println("Inside PC Service Class");
            for (Row row : readRecommendationTable.all()) {
                userTypeParser = new TypeParser();
                userTypeParser.setType(type);
                userTypeParser.setRating(row.getFloat("reco_value"));
                userTypeParser.setMovieName(row.getString("movie_name"));
                System.out.println(row.getFloat("reco_value"));
                System.out.println(row.getString("movie_name"));
                listTypeParser.add(userTypeParser);
            }
        }
        else {
            listTypeParser= new ArrayList<>();
        }
        return listTypeParser;
    }

    @Override
    public List<MovieGenreRating> getGenreCorrelation (String userId){
        String queryGenreRating="select * from genres_rating where genre_name='Western' limit 30 allow filtering";
        final ResultSet readGenreTable=session.execute(queryGenreRating);
        List<MovieGenreRating> ratingList=new ArrayList<>();

        for (Row row : readGenreTable.all()) {
            MovieGenreRating movieGenreRating = new MovieGenreRating(row.getString("genre_name"),
                    row.getDouble("average_rating"), row.getString("movie_name"));
            ratingList.add(movieGenreRating);
        }

        return ratingList;
    }
}
