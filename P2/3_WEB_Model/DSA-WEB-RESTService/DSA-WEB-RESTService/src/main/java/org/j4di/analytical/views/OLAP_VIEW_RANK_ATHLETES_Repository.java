package org.j4di.analytical.views;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OLAP_VIEW_RANK_ATHLETES_Repository extends JpaRepository<OLAP_VIEW_RANK_ATHLETES, Long> {

    @Query("SELECT o FROM OLAP_VIEW_RANK_ATHLETES o")
    List<OLAP_VIEW_RANK_ATHLETES> get_OLAP_VIEW_RANK_ATHLETES();
}

