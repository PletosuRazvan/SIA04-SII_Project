package org.datasource.csv.athletes;

import org.datasource.csv.CSVDataSourceConnector;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

@Service
public class AthleteViewBuilder {
    private static Logger logger = Logger.getLogger(AthleteViewBuilder.class.getName());

    private static final String CSV_PATH = "datasource/athletes.csv";

    private List<AthleteView> athleteViewList;
    private CSVDataSourceConnector dataSourceConnector;

    public AthleteViewBuilder(CSVDataSourceConnector dataSourceConnector) {
        this.dataSourceConnector = dataSourceConnector;
    }

    public List<AthleteView> getAthleteViewList() {
        return athleteViewList;
    }

    public AthleteViewBuilder build() throws Exception {
        List<Map<String, String>> tuples = dataSourceConnector.readCSV(CSV_PATH);
        this.athleteViewList = new ArrayList<>();

        for (Map<String, String> tuple : tuples) {
            AthleteView view = new AthleteView(
                    parseLong(tuple.get("ID")),
                    tuple.get("Name"),
                    tuple.get("Sex"),
                    parseDouble(tuple.get("Height")),
                    parseDouble(tuple.get("Weight"))
            );
            this.athleteViewList.add(view);
        }
        logger.info("Built " + athleteViewList.size() + " AthleteView records");
        return this;
    }

    private Long parseLong(String val) {
        try { return val != null ? Long.parseLong(val) : null; }
        catch (NumberFormatException e) { return null; }
    }
    private Double parseDouble(String val) {
        try { return val != null ? Double.parseDouble(val) : null; }
        catch (NumberFormatException e) { return null; }
    }
}

