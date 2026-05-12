package org.j4di.integration.views;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface OLAP_CONSOLIDARE_OLIMPICA_Repository
        extends JpaRepository<OLAP_CONSOLIDARE_OLIMPICA, Long> {

    @Query("SELECT o FROM OLAP_CONSOLIDARE_OLIMPICA o")
    List<OLAP_CONSOLIDARE_OLIMPICA> get_OLAP_CONSOLIDARE_OLIMPICA();
}

