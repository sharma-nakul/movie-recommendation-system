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

import static org.apache.spark.sql.functions.*;


/**
 * Created by Naks on 02-May-16.
 * Db Operations to save in cassandra
 * CREATE KEYSPACE "movies" WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};
 * CREATE TABLE movies.movies_list (movie_id int PRIMARY KEY, movie_name text, genre_list list<text>);
 * CREATE TABLE movies.ratings (user_id int, movie_id int, rating_given_by_user float, PRIMARY KEY(user_id,movie_id));
 * CREATE TABLE movies.tags (user_id int, movie_id int, tag text, PRIMARY KEY(user_id,movie_id,tag));
 * CREATE TABLE movies.recommendation (movie_id int , movie_name text,reco_value float, PRIMARY KEY(movie_id,reco_value))
 * with clustering order by (reco_value desc);
 * CREATE TABLE movies.genre_correlation (genre_a text, genre_b text, similarity double, PRIMARY KEY(genre_a,genre_b));
 * CREATE TABLE movies.genres_rating (genre_name text, average_rating double, movie_name text,
 * PRIMARY KEY(genre_name, average_rating, movie_name)) with clustering order by (average_rating desc);
 * CREATE TABLE movies.genres (movie_id int PRIMARY KEY, movie_name text,genre text);
 * CREATE TABLE movies.bayesian_avg (movie_id int PRIMARY KEY,movie_name text, average_rating double,count int
 * ,bayesian_average double);
 * CREATE TABLE movies.user_count (user_id int PRIMARY KEY, count int);
 */
public class RecoMining {

    private static final Logger logger = LoggerFactory.getLogger(RecoMining.class);

    private JavaRDD<Movie> moviesRDD;
    private JavaRDD<Rating> ratingRDD;
    private JavaRDD<MovieGenres> moviegenreRDD;
    private SQLContext sqlContext;
    private JavaSparkContext jsc;
    private DBService dbService;


    public RecoMining(SparkContext sc) {
        this.jsc = JavaSparkContext.fromSparkContext(sc);
        sqlContext = new SQLContext(sc);
        this.moviesRDD = jsc.textFile(CONSTANT.getMoviesFilePath()).map(new MapMovieUDF());
        this.ratingRDD = jsc.textFile(CONSTANT.getRatingsFilePath()).map(new MapRatingUDF());
        this.dbService = new DBService();
        moviesRDD.cache();
        ratingRDD.cache();
    }

    public void mapMovieAndRecommendations(List<PCModel> ratingRecommendation) {
        //Convert List<PCModel> to rdd
        JavaRDD<PCModel> recoRDD = jsc.parallelize(ratingRecommendation);
        recoRDD.cache();
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


        //DataFrame GenreDF = sqlContext.sql("SELECT movieName, genreList FROM movieIdAndName");

        JavaRDD<Row> movieRecoDdRDD = joinedDF.toJavaRDD();

        //Convert to RDD to persist in Cassandra
        JavaRDD<MovieRecommendation> movieRecoRDD = movieRecoDdRDD.map(new MapRecoUDF());
        movieRecoRDD.cache();

        dbService.saveRecommendation(movieRecoRDD);
    }

    public void CountUsers() {
        // Fetch Movie Ratings from table
        DataFrame ratingDF = sqlContext.createDataFrame(ratingRDD, Rating.class);
        ratingDF.registerTempTable("movie_ratings");

        DataFrame UserCount = sqlContext.sql("Select userId, CAST(count(userId) as Integer) as count from movie_ratings group by userId");
        UserCount.show();
        //Persist results in cassandra
        JavaRDD<Row> UCount = UserCount.toJavaRDD();
        JavaRDD<UserCount> UCountMap = UCount.map(new UserCountUDF());
        dbService.saveUserCountMap(UCountMap);
    }

    public void SaveGenres() {
        DataFrame schemaMovieDF = sqlContext.createDataFrame(moviesRDD, Movie.class);
        schemaMovieDF.registerTempTable("movieGenreList");
        DataFrame movieDF = sqlContext.sql("SELECT movieId, movieName, genreList FROM movieGenreList");
        DataFrame genres = movieDF.select(movieDF.col("movieId"), movieDF.col("movieName"), org.apache.spark.sql.functions.explode(movieDF.col("genreList").as("genre")));
        DBService saveToCassandra = new DBService();

        JavaRDD<Row> genresRDD = genres.toJavaRDD();
        moviegenreRDD = genresRDD.map(new MapGenresUDF());
        dbService.saveGenreMovieMap(moviegenreRDD);

        genres.show();
    }
    public double GenreCorrelation(String genreA, String genreB) {
        //double GCSimilarity = CONSTANT.getwRatio() * genre_prob(genreA, genreB) + genre_weight(genreA, genreB);
        double GCSimilarity = 0.5;
        DataFrame ratingDF = sqlContext.createDataFrame(ratingRDD, Rating.class);
        ratingDF.registerTempTable("movie_ratings");
        DataFrame GenreSimilarityMap = sqlContext.sql("select '" + genreA + "' as genre_name, '" + genreB + "' as similar_genre, CAST('" + GCSimilarity + "' as double) as similarity_score from movie_ratings limit 1");

        GenreSimilarityMap.show();

        //Persist results in cassandra
        JavaRDD<Row> GSM = GenreSimilarityMap.toJavaRDD();
        JavaRDD<GenreSimilarity> MGRatingMap = GSM.map(new MapGenreCorrelationUDF());

        DataFrame df = sqlContext.createDataFrame(MGRatingMap, GenreSimilarity.class);
        df.show();

        dbService.saveGenreCorrelationMap(MGRatingMap);
        return GCSimilarity;
    }

    private double genre_prob(String genreA, String genreB) {
        DataFrame genresDF = sqlContext.createDataFrame(moviegenreRDD, MovieGenres.class);
        genresDF.registerTempTable("genres");
        double IA = genresDF.sqlContext().sql("select CAST(count(movieId) as double) from genres where genre = '" + genreA + "'").head().getDouble(0);
        DataFrame moviesGenreList = sqlContext.createDataFrame(moviesRDD, Movie.class);
        moviesGenreList.registerTempTable("moviesGenreList");
        //double IB = moviesGenreList.sqlContext().sql("select * from moviesGenreList where  ").head().getDouble(0);
        //System.out.println(genreA + " " + genreB);

        DataFrame genres = moviesGenreList.select(moviesGenreList.col("genreList"), moviesGenreList.col("movieName"), array_contains(moviesGenreList.col("genreList"), genreA).as("genreA"), moviesGenreList.col("movieName"), array_contains(moviesGenreList.col("genreList"), genreB).as("genreB")).where("genreA=true and genreB=true");
        //DataFrame genres = moviesGenreList.select(moviesGenreList.col("movieId").when(org.apache.spark.sql.functions.array_contains(moviesGenreList.col("genreList"),genreB),"true").when(org.apache.spark.sql.functions.array_contains(moviesGenreList.col("genreList"),genreA),"true"),moviesGenreList.col("genreList"));
        //DataFrame genres = moviesGenreList.select(moviesGenreList.col("movieId"),moviesGenreList.col("genreList")).where(moviesGenreList.col("genreList"));
        //genres.show();
        return genres.count() / IA;
    }


    private double genre_weight(String genreA, String genreB) {
        //Register Movie RDD into DataFrame -> Movie DataFrame
        double sumNum=0;
        double sumDnum1=0;
        double sumDnum2=0;
        double genre_weight;
        DataFrame schemaMovieDF = sqlContext.createDataFrame(moviesRDD, Movie.class);
        schemaMovieDF.registerTempTable("movies");
        DataFrame moviesGenreList = sqlContext.createDataFrame(moviesRDD, Movie.class);
        DataFrame ratingDF = sqlContext.createDataFrame(ratingRDD, Rating.class);
        DataFrame genresDF = sqlContext.createDataFrame(moviegenreRDD, MovieGenres.class);
        //schemaMovieDF.registerTempTable("movieIdAndName");
        //DataFrame movieDF = sqlContext.sql("SELECT movieId, movieName FROM movieIdAndName");
        DataFrame movieDF = schemaMovieDF.join(ratingDF, "movieId").select("movieId","movieName", "ratingGivenByUser");
        movieDF.show();
        Double AvgGenreA = averageofGenre(genreA,genresDF,ratingDF);
        Double AvgGenreB = averageofGenre(genreB,genresDF,ratingDF);
        Double PenaltyAB = 1.0;
        Double AvgofMovieI = 1.0;

        List<Row> movieList=schemaMovieDF.collectAsList();
        for(Row row: movieList){
            AvgofMovieI = averageOfMovie(row.getInt(1),ratingDF);
            PenaltyAB = penalty(row.getString(2),moviesGenreList);
            Double Part1 = PenaltyAB*(AvgofMovieI-AvgGenreA);
            Double Part2 = PenaltyAB*(AvgofMovieI-AvgGenreB);
            sumNum = sumNum + Part1*Part2;
        }
        for(Row row: movieList){
            AvgofMovieI = averageOfMovie(row.getInt(1),ratingDF);
            PenaltyAB = penalty(row.getString(2),moviesGenreList);
            Double Part1 = PenaltyAB*(AvgofMovieI-AvgGenreA);
            sumDnum1 = sumDnum1 + (Part1*Part1);
        }
        for(Row row: movieList){
            AvgofMovieI = averageOfMovie(row.getInt(1),ratingDF);
            PenaltyAB = penalty(row.getString(2),moviesGenreList);
            Double Part2 = PenaltyAB*(AvgofMovieI-AvgGenreB);
            sumDnum2 = sumDnum2 + Part2*Part2;
        }

        genre_weight = sumNum/(Math.sqrt(sumDnum1)*Math.sqrt(sumDnum1));
        System.out.println(genre_weight);
        return 1;
    }

    private double averageOfMovie(int movieId,DataFrame ratingDF)
    {

        ratingDF.registerTempTable("movie_ratings");
        double averageMovie = ratingDF.groupBy("movieId").agg(avg("ratingGivenByUser")).where("movieId="+movieId+"").head().getInt(0);
        return averageMovie;
    }

    private double averageofGenre(String genreName, DataFrame genresDF, DataFrame movieDF)
    {
        genresDF.registerTempTable("genres");
        Double averageMovie = genresDF.join(movieDF,"movieId").groupBy("genre").agg(avg("ratingGivenByUser")).as("AvgRating").where("genre='"+genreName+"'").head().getDouble(1);
        //DataFrame unionFrame = genresDF.join(movieDF,"movieId");
        //unionFrame.show();
        //DataFrame averageMovie = genresDF.join(movieDF,"movieId").groupBy("genre").agg(avg("ratingGivenByUser")).where("genre='"+genreName+"'");
        //averageMovie.show();
        return averageMovie;
    }

    private double penalty(String movie,DataFrame moviesGenreList) {
        double IA = moviesGenreList.select(size(moviesGenreList.col("genreList")), moviesGenreList.col("movieName")).where("movieName='" + movie + "'").head().getInt(0);
        //System.out.println(IA);
        return 2 / IA;
    }

    public void GenreMoviesRatings() {

        // Fetch Movie Ratings from table
        DataFrame ratingDF = sqlContext.createDataFrame(ratingRDD, Rating.class);
        ratingDF.registerTempTable("movie_ratings");

        //Fetch MovieId and MovieName from table
        DataFrame schemaMovieDF = sqlContext.createDataFrame(moviesRDD, Movie.class);
        schemaMovieDF.registerTempTable("movieIdAndName");
        DataFrame movieDF = sqlContext.sql("SELECT movieId, movieName,genreList FROM movieIdAndName");

        //Calculate Bayesian Average
        double meanOfAllMovieRatings = ratingDF.agg(avg("ratingGivenByUser")).alias("meanOfMovieRating").head().getDouble(0);
        DataFrame tDF = ratingDF.groupBy("movieId").agg(avg("ratingGivenByUser").alias("averageRating"),
                count("movieId").alias("count")).orderBy("count");
        tDF.registerTempTable("tDF");


        DataFrame genres = movieDF.select(movieDF.col("movieId"), movieDF.col("movieName"), org.apache.spark.sql.functions.explode(movieDF.col("genreList")).as("genre"));
        genres.registerTempTable("genres");

        DataFrame AvgMovieRating = sqlContext.sql("select CAST(movieIdAndName.movieId as Integer), movieIdAndName.movieName, averageRating " +
                "from " + "movieIdAndName inner join tDF on movieIdAndName.movieId = tDF.movieId " +
                "order by averageRating desc");
        AvgMovieRating.registerTempTable("AvgMovieRating");

        DataFrame MovieGenreRatingMap = sqlContext.sql("select genres.genre, AvgMovieRating.averageRating, genres.movieName from genres " +
                "FULL OUTER JOIN AvgMovieRating on genres.movieId = AvgMovieRating.movieId order by genres.genre, " +
                "AvgMovieRating.averageRating desc");

        MovieGenreRatingMap.show();

        //Persist results in cassandra
        JavaRDD<Row> MGRating = MovieGenreRatingMap.toJavaRDD();
        JavaRDD<MovieGenreRating> MGRatingMap = MGRating.map(new MapGenresAvgRatingsUDF());
        dbService.saveGenreMovieRatingMap(MGRatingMap);




        /*String sqlQuery = "select movieId, averageRating, count, ((count/(count+" + CONSTANT.getMinimumVotesRequired() +
                "))*averageRating) + ((" + CONSTANT.getMinimumVotesRequired() + "/CAST((count+" + CONSTANT.getMinimumVotesRequired() +
                ") AS Decimal))*" + meanOfAllMovieRatings + ") AS bayesianAverage from tDF";
        DataFrame bayesianAvgDF = sqlContext.sql(sqlQuery);
        bayesianAvgDF.registerTempTable("bayesianAvgDF");

        DataFrame bayesianDF = sqlContext.sql("select CAST(movieIdAndName.movieId as Integer), " +
                "movieIdAndName.movieName, averageRating, CAST(count as Integer), " +
                "bayesianAverage from movieIdAndName inner join bayesianAvgDF on movieIdAndName.movieId = bayesianAvgDF.movieId " +
                "order by bayesianAverage desc");

        bayesianDF.show();*/
       /* //Persist results in cassandra
        JavaRDD<Row> bayesianAverageRDD = bayesianDF.toJavaRDD();
        JavaRDD<BayesianAverage> bayesianRDD = bayesianAverageRDD.map(new MapBayesianUDF());
        dbService.saveBayesianAverage(bayesianRDD);*/



    }

    public void BayesianAverageCalculation() {
        /**
         * StepsA
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
