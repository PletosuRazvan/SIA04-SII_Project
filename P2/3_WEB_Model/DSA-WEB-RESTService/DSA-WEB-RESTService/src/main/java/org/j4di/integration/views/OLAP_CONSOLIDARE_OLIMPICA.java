package org.j4di.integration.views;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

/**
 * Maps to OLAP_CONSOLIDARE_OLIMPICA Spark SQL view via Hive JDBC.
 */
@Getter
@Entity
@Immutable
@Table(name = "OLAP_CONSOLIDARE_OLIMPICA")
public class OLAP_CONSOLIDARE_OLIMPICA {
    @Id
    private Long Athlete_ID;
    private String Athlete_Name;
    private String Sex;
    private String Sport;
    private String Event;
    private String Medal;
    private Long Year;
    private String Season;
    private String City;
    private String NOC;
    private String Country_Name;
}

