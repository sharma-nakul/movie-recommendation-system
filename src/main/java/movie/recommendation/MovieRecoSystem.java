package movie.recommendation;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import movie.model.CONSTANT;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.apache.spark.api.java.JavaSparkContext.fromSparkContext;

public class MovieRecoSystem {

    private static final String ratingsFilePath = "spark-streaming\\src\\main\\resources\\ratings.csv";
    private static final String moviesFilePath = "spark-streaming\\src\\main\\resources\\movies.csv";

    public static void main(String args[]) {
        try {

            SparkConf sparkConf = new SparkConf().setAppName("MovieRecommendation")
                    .setMaster("local[4]")
                    .set("spark.driver.allowMultipleContexts", "true")
                    .set("spark.cassandra.connection.host", "127.0.0.1");

            SparkContext sc = new SparkContext(sparkConf);
            JavaSparkContext ctx = fromSparkContext(sc);
            SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

            //Creating Cluster object
            Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

            //Creating Session object
            Session session = cluster.connect(CONSTANT.getKeySpace());

            DataModel model = new FileDataModel(new File(ratingsFilePath));
            UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
            UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.8, similarity, model);
            UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
            List<RecommendedItem> recommendations = recommender.recommend(7, 10);


            BufferedReader br = new BufferedReader(new FileReader(moviesFilePath));
            String line = null;
            HashMap<Long, String> recMap = new HashMap<>();


            while ((line = br.readLine()) != null) {
                String str[] = line.split(",");
                recMap.put(Long.valueOf(str[0]), str[1]);
            }
            //System.out.println(recMap.get("1"));
            for (RecommendedItem recommendation : recommendations) {
                System.out.println(recommendation);

                String query = "Insert into " + CONSTANT.getRecoTable() + " (movie_id, movie_name, reco_value) VALUES (" + recommendation.getItemID() + ", '" +
                        recMap.get(recommendation.getItemID()) + "', " + recommendation.getValue() + ");";
                session.execute(query);
            }

            /*-------------------------Genre-------------------------------------------------*/

            HashMap<String, ArrayList<String>> movieGenreMap = new HashMap<>();

            final ResultSet rows = session.execute("SELECT * FROM movies_list");

           /* ArrayList<Movie> results = new ArrayList<>();
            for (Row row : rows.all()) {
                List<String> genList = row.getList("genre_list", String.class);
                results.add(new Movie(row.getString("movie_id"), row.getString("movie_name"), genList));
            }
            Collections.sort(results, (a, b) ->
                    b.getMovieId().compareTo(a.getMovieId()));
            for(Movie m: results)
                System.out.println(m.getMovieId()+":"+m.getMovieName());*/

            /*---------------Read Recommedation from Cassandra-------------------------------*/

          /*  final ResultSet rows1 = session.execute("SELECT * FROM recommendation");

            ArrayList<MovieRecommendation> results1 = rows.all().stream().map(row -> new MovieRecommendation(row.getInt("movie_id"),
                    row.getString("movie_name"), row.getFloat("reco_value"))).collect(Collectors.toCollection(ArrayList::new));
            *//*  Collections.sort(results, (a, b) ->
                    b.getMovieId().compareTo(a.getMovieId()));*//*
            for(MovieRecommendation m: results1)
                System.out.println(m.getMovieId()+":"+m.getMovieName()+":"+m.getRecoValue());*/

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
