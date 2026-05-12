package org.j4di.analytical.views;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

/**
 * Maps to OLAP_VIEW_RANK_ATHLETES Spark SQL view.
 * Ranking of athletes by medal count per sport.
 */
@Getter
@Entity
@Immutable
@Table(name = "OLAP_VIEW_RANK_ATHLETES")
public class OLAP_VIEW_RANK_ATHLETES {
    @Id
    private String Athlete_Name;
    private String Sport;
    private Long Medals;
    private Long Rank_In_Sport;
}

