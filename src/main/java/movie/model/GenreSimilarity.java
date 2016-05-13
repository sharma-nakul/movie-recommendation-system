package movie.model;

/**
 * Created by Shounak on 5/2/2016.
 */
public class GenreSimilarity {
    private String genreA;
    private String genreB;
    private double similarity;

    public GenreSimilarity(String genreA, String genreB, double similarity) {
        this.genreA = genreA;
        this.genreB = genreB;
        this.similarity = similarity;
    }

    public String getGenreA() {
        return genreA;
    }

    public void setGenreA(String genreA) {
        this.genreA = genreA;
    }

    public String getGenreB() {
        return genreB;
    }

    public void setGenreB(String genreB) {
        this.genreB = genreB;
    }

    public double getSimilarity() {
        return similarity;
    }

    public void setSimilarity(double similarity) {
        this.similarity = similarity;
    }
}