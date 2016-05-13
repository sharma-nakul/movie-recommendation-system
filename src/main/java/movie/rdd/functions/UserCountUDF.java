package movie.rdd.functions;

import movie.model.UserCount;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

/**
 * Created by Shounak on on 2nd May 2016
 * To map store 1-1 mapping of genre and movies and ratings
 */
public class UserCountUDF implements Function<Row,UserCount> {
    @Override
    public UserCount call(Row row) throws Exception{
        return new UserCount(row.getInt(0),row.getInt(1));
    }
}