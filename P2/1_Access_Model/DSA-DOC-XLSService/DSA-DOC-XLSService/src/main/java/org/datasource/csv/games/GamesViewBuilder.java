package org.datasource.csv.games;

import org.datasource.csv.CSVDataSourceConnector;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Service
public class GamesViewBuilder {
    private static Logger logger = Logger.getLogger(GamesViewBuilder.class.getName());

    private static final String CSV_PATH = "datasource/games.csv";

    private List<GamesView> gamesViewList;
    private CSVDataSourceConnector dataSourceConnector;

    public GamesViewBuilder(CSVDataSourceConnector dataSourceConnector) {
        this.dataSourceConnector = dataSourceConnector;
    }

    public List<GamesView> getGamesViewList() {
        return gamesViewList;
    }

    public GamesViewBuilder build() throws Exception {
        List<Map<String, String>> tuples = dataSourceConnector.readCSV(CSV_PATH);
        this.gamesViewList = new ArrayList<>();

        for (Map<String, String> tuple : tuples) {
            GamesView view = new GamesView(
                    tuple.get("Games"),
                    parseInteger(tuple.get("Year")),
                    tuple.get("Season"),
                    tuple.get("City")
            );
            this.gamesViewList.add(view);
        }
        logger.info("Built " + gamesViewList.size() + " GamesView records");
        return this;
    }

    private Integer parseInteger(String val) {
        try { return val != null ? Integer.parseInt(val) : null; }
        catch (NumberFormatException e) { return null; }
    }
}

