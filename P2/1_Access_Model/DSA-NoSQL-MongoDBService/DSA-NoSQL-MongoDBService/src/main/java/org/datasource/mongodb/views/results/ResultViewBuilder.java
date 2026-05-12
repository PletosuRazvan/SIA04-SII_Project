package org.datasource.mongodb.views.results;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.datasource.mongodb.MongoDataSourceConnector;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

@Service
public class ResultViewBuilder {
    private static Logger logger = Logger.getLogger(ResultViewBuilder.class.getName());

    // Data cache
    private List<ResultView> resultViewList;

    public List<ResultView> getResultViewList() {
        return resultViewList;
    }

    private MongoDataSourceConnector dataSourceConnector;

    public ResultViewBuilder(MongoDataSourceConnector dataSourceConnector) {
        this.dataSourceConnector = dataSourceConnector;
    }

    // Builder Workflow
    public ResultViewBuilder build() throws Exception {
        return this.select();
    }

    public ResultViewBuilder select() throws Exception {
        MongoDatabase db = dataSourceConnector.getMongoDatabase();

        MongoCollection<Document> resultsCollection =
                db.getCollection("results");

        this.resultViewList = new ArrayList<>();
        for (Document doc : resultsCollection.find()) {
            ResultView view = new ResultView(
                    doc.get("Athlete_ID") != null ? ((Number) doc.get("Athlete_ID")).longValue() : null,
                    doc.getString("Games"),
                    doc.getString("NOC"),
                    doc.getString("Sport"),
                    doc.getString("Event"),
                    doc.getString("Medal"),
                    doc.get("Age") != null ? ((Number) doc.get("Age")).intValue() : null
            );
            this.resultViewList.add(view);
        }

        logger.info("Loaded " + resultViewList.size() + " results from MongoDB");
        return this;
    }
}

