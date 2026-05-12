package org.j4di.analytical.views;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OLAP_VIEW_CUBE_COUNTRY_SPORT_Repository extends JpaRepository<OLAP_VIEW_CUBE_COUNTRY_SPORT, Long> {

    @Query("SELECT o FROM OLAP_VIEW_CUBE_COUNTRY_SPORT o")
    List<OLAP_VIEW_CUBE_COUNTRY_SPORT> get_OLAP_VIEW_CUBE_COUNTRY_SPORT();
}

