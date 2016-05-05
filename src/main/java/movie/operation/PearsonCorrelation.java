package movie.operation;

import movie.model.CONSTANT;
import movie.model.PCModel;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Naks on 02-May-16.
 * Class to implement Pearson Correlation Algorithm
 */
public class PearsonCorrelation {

    private static final Logger logger = LoggerFactory.getLogger(PearsonCorrelation.class);

    public List<PCModel> applyOnRatings() {
        try {
            DataModel model = new FileDataModel(new File(CONSTANT.getRatingsFilePath()));
            UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
            UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.8, similarity, model);
            UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);
            List<RecommendedItem> recommendations = recommender.recommend(7, 10);
            List<PCModel> recoList=new ArrayList<>();
            for(RecommendedItem rI:recommendations)
            {
                PCModel pc=new PCModel((int)rI.getItemID(),rI.getValue());
                recoList.add(pc);
            }
            return recoList;
        } catch (IOException | TasteException e) {
            logger.error("IOException/TasteException generated in PersonCorrelation.class -> applyOnRatings");
            e.printStackTrace();
            return null;
        } catch (Exception e) {
            logger.error("Exception generated in PersonCorrelation.class -> applyOnRatings");
            e.printStackTrace();
            return null;
        }
    }
}
