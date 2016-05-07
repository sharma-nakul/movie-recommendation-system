package movie.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import movie.model.CONSTANT;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * Created by Naks on 02-May-16.
 * Configuration file for Cassandra
 */

@Configuration
public class CassandraSessionManager {

    private Session session;
    private Cluster cluster;

    public CassandraSessionManager() {

    }

    public Session getSession() {
        return session;
    }

    @PostConstruct
    public void initIt() {
        cluster = Cluster.builder().addContactPoint(
                CONSTANT.getCassandraHost()).build();
        session = cluster.connect(CONSTANT.getKeySpace());
    }

    @PreDestroy
    public void destroy() {
        if (session != null) {
            session.close();
        }
        if (cluster != null) {
            cluster.close();
        }
    }

}
