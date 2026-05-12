package org.j4di.analytical.views;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

/**
 * Maps to OLAP_VIEW_COUNTRY_SPORT_ATHLETE Spark SQL view.
 * ROLLUP: Country > Sport > Athlete medal hierarchy.
 */
@Getter
@Entity
@Immutable
@Table(name = "OLAP_VIEW_COUNTRY_SPORT_ATHLETE")
public class OLAP_VIEW_COUNTRY_SPORT_ATHLETE {
    @Id
    private String Country_Name;
    private String Sport;
    private String Athlete_Name;
    private Long Total_Medals;
}

