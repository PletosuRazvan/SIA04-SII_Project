package org.j4di.integration.views;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

/**
 * Maps to OLAP_FACTS_MEDALS Spark SQL view via Hive JDBC.
 */
@Getter
@Entity
@Immutable
@Table(name = "OLAP_FACTS_MEDALS")
public class OLAP_FACTS_MEDALS {
    @Id
    private Long Athlete_ID;
    private String Sport;
    private Long Year;
    private String NOC;
    private String Country_Name;
    private Long Total_Participations;
    private Long Gold_Medals;
    private Long Silver_Medals;
    private Long Bronze_Medals;
    private Long Total_Medals;
}

