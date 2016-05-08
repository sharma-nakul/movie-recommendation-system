package movie.config;

import org.apache.log4j.Level;
import org.apache.spark.SparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;


@Configuration
@ComponentScan(basePackages = "movie")
@EnableAutoConfiguration
public class SparkApp {

    @Autowired
    private SparkContext sc;

    public static void main(String[] args) throws Exception {

        org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.OFF);

        /*SparkConf sparkConf = new SparkConf().setAppName("MovieRecommendation")
                .setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.cassandra.connection.host", "127.0.0.1");

        SparkContext sc=new SparkContext(sparkConf);*/

//        JavaSparkContext jsc = fromSparkContext(sc);
     //   SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

       // RecoMining recoMining = new RecoMining(sc);
        //saveInCassandra.saveMovies(); // Completed and Working
        //saveInCassandra.saveRatings(); // Completed and Working
        //saveInCassandra.saveTags(); // Completed and Working

        try {
            // TODO: 5/6/2016  changing according to functionality, based on user request the required function must be called
            //PearsonCorrelation pc = new PearsonCorrelation();
            //List<PCModel> recommendationBasedOnUserRatings = pc.applyOnRatings(7,50,0.8);
            //recoMining.mapMovieAndRecommendations(recommendationBasedOnUserRatings);
            //recoMining.SaveGenres();
            //System.out.println("The GC for Comedy and Romance is: "+recoMining.GenreCorrelation("Comedy","Romance"));
            //recoMining.BayesianAverageCalculation();
         //   recoMining.genre_weight("Comedy","Romance");
        }
        catch (NullPointerException e)
        {
            e.printStackTrace();
        }
    }
}