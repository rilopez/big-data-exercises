package nearsoft.academy.bigdata.recommendation;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
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

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class MovieRecommender {


    private final GenericUserBasedRecommender recommender;
    private int totalReviews;
    private HashBiMap<String, Integer> users;
    private HashBiMap<String, Integer> products;

    public MovieRecommender(String dataSource) throws IOException, TasteException {
            DataModel model = new FileDataModel(new File(convertSourceToCsv(dataSource)));
            UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
            UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
            recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);

    }
    
    
    public List<String> getRecommendationsForUser(String userName) throws TasteException {
        System.out.println("recommendations for user 1 - " + users.inverse().get(1));
        List<RecommendedItem> recommendations = recommender.recommend(users.get(userName), 3);
        List<String> recommendedMovies = new ArrayList<>();
        BiMap<Integer, String> productsByName = products.inverse();
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);

            String movieName = productsByName.get((int)recommendation.getItemID());
            System.out.println("movie:"+movieName);
            recommendedMovies.add(movieName);
        }
        
        
        return recommendedMovies;
    }

    private String convertSourceToCsv(String dataSource) {
        try {

            File source = new File(dataSource);
            users= HashBiMap.create ();
            products= HashBiMap.create();

            File temp = new File(source.getParentFile().getAbsolutePath() + "/tempdata.csv");
            if (temp.exists()) {
                temp.delete();
               //return temp.getAbsolutePath();
            }
            else{
                temp.createNewFile();
            }

            try (InputStream fileStream = new FileInputStream(dataSource);
                 InputStream gzipStream = new GZIPInputStream(fileStream);
                 Reader decoder = new InputStreamReader(gzipStream, "UTF8");
                 BufferedReader buffered = new BufferedReader(decoder);
                 Writer writer = new BufferedWriter(new FileWriter(temp));

            ) {
                String str;
                Integer userId = null;
                String score = "";
                Integer productId = null;
                totalReviews = 0;
                boolean readingRecord = false;
                while ((str = buffered.readLine()) != null) {
                    if (readingRecord) {
                        if (str.contains("review/userId")) {
                            String userName =getValuePortionOfString(str);
                            if(!users.containsKey(userName)){
                                users.put(userName,users.size()+1);
                            }
                            userId = users.get(userName);
                                    
                        } else if (str.contains("review/score")) {
                            score = getValuePortionOfString(str);
                        } else if (str.contains("review/summary")) {
                            

                            writer.append(String.valueOf(userId));
                            writer.append(",");
                            writer.append(String.valueOf(productId));
                            writer.append(",");
                            writer.append(score);
                            writer.append("\n");

                            userId = null;
                            score = "";
                            productId = null;
                            readingRecord = false;
                            

                        }
                    } else if (str.contains("product/productId")) {
                        String productName = getValuePortionOfString(str);
                        if(!products.containsKey(productName)){
                            products.put(productName,products.size()+1);
                        }
                        productId = products.get(productName);
                        readingRecord = true;
                        totalReviews++;
                    }

                }
            }
            return temp.getAbsolutePath();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error processing the data source " + dataSource, e);
        }
    }

    private String getValuePortionOfString(String str) {
        return str.substring(str.indexOf(":") + 2, str.length());
    }

    public int getTotalProducts(){
        return products.size();
    }

    public int getTotalUsers(){
        return users.size();
    }


    public int getTotalReviews() {
        return totalReviews;
    }

    public void setTotalReviews(int totalReviews) {
        this.totalReviews = totalReviews;
    }
}
