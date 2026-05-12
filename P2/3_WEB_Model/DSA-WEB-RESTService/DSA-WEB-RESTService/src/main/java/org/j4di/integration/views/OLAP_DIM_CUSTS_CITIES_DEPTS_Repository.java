package org.j4di.integration.views;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OLAP_DIM_CUSTS_CITIES_DEPTS_Repository
        extends JpaRepository<OLAP_DIM_CUSTS_CITIES_DEPTS, Long> {

    @Query("SELECT o FROM OLAP_DIM_CUSTS_CITIES_DEPTS o")
    List<OLAP_DIM_CUSTS_CITIES_DEPTS> get_OLAP_DIM_CUSTS_CITIES_DEPTS();
}