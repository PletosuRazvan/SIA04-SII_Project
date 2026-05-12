package org.datasource.mongodb.views.results;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ResultView implements Serializable {
    @JsonProperty("Athlete_ID")
    private Long athleteId;
    @JsonProperty("Games")
    private String games;
    @JsonProperty("NOC")
    private String noc;
    @JsonProperty("Sport")
    private String sport;
    @JsonProperty("Event")
    private String event;
    @JsonProperty("Medal")
    private String medal;
    @JsonProperty("Age")
    private Integer age;

    public ResultView() {}

    public ResultView(Long athleteId, String games, String noc, String sport, String event, String medal, Integer age) {
        this.athleteId = athleteId;
        this.games = games;
        this.noc = noc;
        this.sport = sport;
        this.event = event;
        this.medal = medal;
        this.age = age;
    }

    public Long getAthleteId() { return athleteId; }
    public void setAthleteId(Long athleteId) { this.athleteId = athleteId; }
    public String getGames() { return games; }
    public void setGames(String games) { this.games = games; }
    public String getNoc() { return noc; }
    public void setNoc(String noc) { this.noc = noc; }
    public String getSport() { return sport; }
    public void setSport(String sport) { this.sport = sport; }
    public String getEvent() { return event; }
    public void setEvent(String event) { this.event = event; }
    public String getMedal() { return medal; }
    public void setMedal(String medal) { this.medal = medal; }
    public Integer getAge() { return age; }
    public void setAge(Integer age) { this.age = age; }
}
