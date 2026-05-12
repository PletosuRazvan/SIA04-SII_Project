package org.j4di.analytical.views;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

/**
 * Maps to OLAP_VIEW_CUBE_COUNTRY_SPORT Spark SQL view.
 * CUBE analysis: Country x Sport medal totals.
 */
@Getter
@Entity
@Immutable
@Table(name = "OLAP_VIEW_CUBE_COUNTRY_SPORT")
public class OLAP_VIEW_CUBE_COUNTRY_SPORT {
    @Id
    private String Country_Name;
    private String Sport;
    private Long Nr_Medals;
}

