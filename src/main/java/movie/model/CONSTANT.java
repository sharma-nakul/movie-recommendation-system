package movie.model;

import org.springframework.stereotype.Component;

/**
 * Created by Naks on 02-May-16.
 * To serve static values
 */

@Component
public class CONSTANT {
    //Added by shounak on 5th May 2015 for calculating Genre Similarity
    private static final double wRatio=0.5;
    //Added by shounak on 5th May 2015
    private static final String keySpace="movies";
    private static final String movieListTable="movies_list";
    private static final String ratingsTable="ratings";
    private static final String tagsTable="tags";
    private static final String recoTable="recommendation";
    private static final String bayesianTable="bayesian_avg";
    //Added by shounak on 5th May 2015 for 1-1 genre-movie mapping
    private static final String genresTable="genres";
    //Added by shounak on 5th May 2015
    private static final int minimumVotesRequired=5;

    private static final String moviesFilePath = "src\\main\\resources\\movies.csv";
    private static final String ratingsFilePath = "src\\main\\resources\\ratings.csv";
    private static final String tagsFilePath = "src\\main\\resources\\tags.csv";
    private static final String outputPath = "src\\main\\resources\\output";

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
