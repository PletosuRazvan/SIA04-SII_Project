package org.j4di.analytical.views;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OLAP_VIEW_COUNTRY_SPORT_ATHLETE_Repository extends JpaRepository<OLAP_VIEW_COUNTRY_SPORT_ATHLETE, Long> {

    @Query("SELECT o FROM OLAP_VIEW_COUNTRY_SPORT_ATHLETE o")
    List<OLAP_VIEW_COUNTRY_SPORT_ATHLETE> get_OLAP_VIEW_COUNTRY_SPORT_ATHLETE();
}

