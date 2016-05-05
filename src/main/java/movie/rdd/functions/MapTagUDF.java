package movie.rdd.functions;

import movie.model.Tag;
import org.apache.spark.api.java.function.Function;

/**
 * Created by Naks on 02-May-16.
 * Map CSV file data with Java RDD
 */
public class MapTagUDF implements Function<String,Tag>{

    @Override
    public Tag call(String line) throws Exception{
        Tag tag =new Tag();
        String[] t=line.split(",");
        tag.setUserId(Integer.parseInt(t[0]));
        tag.setMovieId(Integer.parseInt(t[1]));
        tag.setTag(t[2]);
        return tag;
    }
}
