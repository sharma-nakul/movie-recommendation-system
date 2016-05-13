package movie.rdd.functions;

import movie.model.GenreSimilarity;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * Created by Shounak on on 2nd May 2016
 * To map store 1-1 mapping of genre and movies and ratings
 */
public class MapGenreCorrelationUDF implements Function<Row,GenreSimilarity> {
    @Override
    public GenreSimilarity call(Row row) throws Exception{
        try{
            return new GenreSimilarity(row.getString(0),row.getString(1),row.getDouble(2));
        }
        catch(NullPointerException e){
            return new GenreSimilarity(row.getString(0),row.getString(1),row.getDouble(2));
        }
    }
}