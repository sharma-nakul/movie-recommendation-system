package movie.config;

import com.datastax.driver.core.Session;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.View;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.config.annotation.DefaultServletHandlerConfigurer;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.view.BeanNameViewResolver;
import org.springframework.web.servlet.view.InternalResourceViewResolver;
import org.springframework.web.servlet.view.json.MappingJackson2JsonView;

/**
 * Created by Naks on 02-May-16.
 * Web Configuration file
 */
@Configuration
@EnableWebMvc
public class AppConfig extends WebMvcConfigurerAdapter {

    @Bean
    public InternalResourceViewResolver internalResourceViewResolver() {
        InternalResourceViewResolver internalResourceViewResolver = new InternalResourceViewResolver();
        internalResourceViewResolver.setPrefix("/");
        internalResourceViewResolver.setSuffix(".jsp");
        return internalResourceViewResolver;
    }

    @Override
    public void configureDefaultServletHandling(DefaultServletHandlerConfigurer configurer) {
        configurer.enable();
    }

    @Bean
    public View jsonTemplate() {
        MappingJackson2JsonView view = new MappingJackson2JsonView();
        view.setPrettyPrint(true);
        return view;
    }

    @Bean
    public SparkContext sparkContext(){
        SparkConf sparkConf = new SparkConf().setAppName("MovieRecommendation")
                .setMaster("local[*]")
                .set("spark.driver.allowMultipleContexts", "true")
                .set("spark.cassandra.connection.host", "127.0.0.1");

        return new SparkContext(sparkConf);
    }

    @Bean
    public ViewResolver viewResolver() {
        return new BeanNameViewResolver();
    }

    @Bean
    public Session session() {
        return sessionManager().getSession();
    }

    @Bean
    public CassandraSessionManager sessionManager() {
        return new CassandraSessionManager();
    }
}
