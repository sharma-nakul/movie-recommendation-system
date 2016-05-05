package movie.config;

import movie.model.PCModel;
import movie.operation.PearsonCorrelation;
import movie.operation.RecoMining;
import org.apache.log4j.Level;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static org.apache.spark.api.java.JavaSparkContext.fromSparkContext;


@Configuration
@ComponentScan(basePackages = "movie")
@EnableAutoConfiguration
public class SparkApp {

    public static void main(String[] args) throws Exception {

        org.apache.log4j.Logger.getLogger("org").setLevel(Level.OFF);
        org.apache.log4j.Logger.getLogger("akka").setLevel(Level.OFF);
        SpringApplication.run(SparkApp.class,args);

        SparkConf sparkConf = new SparkConf().setAppName("MovieRecommendation")
                .setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.cassandra.connection.host", "127.0.0.1");

        SparkContext sc=new SparkContext(sparkConf);
        JavaSparkContext jsc = fromSparkContext(sc);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sc);

        RecoMining recoMining = new RecoMining(sc);
        //saveInCassandra.saveMovies(); // Completed and Working
        //saveInCassandra.saveRatings(); // Completed and Working
        //saveInCassandra.saveTags(); // Completed and Working

        try {
            PearsonCorrelation pc = new PearsonCorrelation();
            List<PCModel> recommendationBasedOnUserRatings = pc.applyOnRatings();
            //recoMining.mapMovieAndRecommendations(recommendationBasedOnUserRatings);
            recoMining.BayesianAverageCalculation();
        }
        catch (NullPointerException e)
        {
            e.printStackTrace();
        }
    }
}