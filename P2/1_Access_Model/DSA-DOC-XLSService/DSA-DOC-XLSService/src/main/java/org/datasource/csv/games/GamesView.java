package org.datasource.csv.games;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor(force = true)
public class GamesView {
    private String games;
    private Integer year;
    private String season;
    private String city;

    public GamesView(String games, Integer year, String season, String city) {
        this.games = games;
        this.year = year;
        this.season = season;
        this.city = city;
    }
}
