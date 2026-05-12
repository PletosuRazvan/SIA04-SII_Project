package org.datasource.xlsx;

import com.poiji.annotation.ExcelCellName;
import com.poiji.annotation.ExcelSheet;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data @NoArgsConstructor(force = true)
@ExcelSheet("Games")
public class GamesView {
    @ExcelCellName("Games")
    private String games;

    @ExcelCellName("Year")
    private Integer year;

    @ExcelCellName("Season")
    private String season;

    @ExcelCellName("City")
    private String city;

    public GamesView(String games, Integer year, String season, String city) {
        this.games = games;
        this.year = year;
        this.season = season;
        this.city = city;
    }

    public String getGames() { return games; }
    public Integer getYear() { return year; }
    public String getSeason() { return season; }
    public String getCity() { return city; }
}

