package movie.model;

import org.springframework.stereotype.Component;

/**
 * Created by Naks on 02-May-16.
 * To serve static values
 */

@Component
public class CONSTANT {
    private static final double wRatio = 0.5;
    private static final String keySpace = "movies";
    private static final String movieListTable = "movies_list";
    private static final String ratingsTable = "ratings";
    private static final String tagsTable = "tags";
    private static final String recoTable = "recommendation";
    private static final String bayesianTable = "bayesian_avg";
    private static final String genresTable = "genres";
    private static final String userInfoTable= "user_info";
    private static final int minimumVotesRequired = 5;
    private static final String cassandraHost = "127.0.0.1";

    private static final int topRowFromBayesianTable=10;
    private static final int topRowFromRecoTable=10;

    //Pearson Correlation constants
    private static final double accuracyThreshold=0.8;

    private static final String moviesFilePath = "C:\\Users\\Naks\\Google Drive\\CMPE 239\\project239\\movie-recommendation-system\\src\\main\\resources\\movies.csv";
    private static final String ratingsFilePath = "C:\\Users\\Naks\\Google Drive\\CMPE 239\\project239\\movie-recommendation-system\\src\\main\\resources\\ratings.csv";
    private static final String tagsFilePath = "C:\\Users\\Naks\\Google Drive\\CMPE 239\\project239\\movie-recommendation-system\\src\\main\\resources\\tags.csv";
    private static final String outputPath = "C:\\Users\\Naks\\Google Drive\\CMPE 239\\project239\\movie-recommendation-system\\src\\main\\resources\\output";

    public static int getTopRowFromRecoTable() {
        return topRowFromRecoTable;
    }

    public static double getAccuracyThreshold() {
        return accuracyThreshold;
    }

    public static String getUserInfoTable() {
        return userInfoTable;
    }

    public static int getTopRowFromBayesianTable() {
        return topRowFromBayesianTable;
    }

    public static String getCassandraHost() {
        return cassandraHost;
    }

    public static String getGenresTable() {
        return genresTable;
    }

    public static double getwRatio() {
        return wRatio;
    }

    public static String getBayesianTable() {
        return bayesianTable;
    }

    public static int getMinimumVotesRequired() {
        return minimumVotesRequired;
    }

    public static String getMoviesFilePath() {
        return moviesFilePath;
    }

    public static String getOutputPath() {
        return outputPath;
    }

    public static String getRatingsFilePath() {
        return ratingsFilePath;
    }

    public static String getTagsFilePath() {
        return tagsFilePath;
    }

    public static String getKeySpace() {
        return keySpace;
    }

    public static String getMovieListTable() {
        return movieListTable;
    }

    public static String getRatingsTable() {
        return ratingsTable;
    }

    public static String getTagsTable() {
        return tagsTable;
    }

    public static String getRecoTable() {
        return recoTable;
    }
}
