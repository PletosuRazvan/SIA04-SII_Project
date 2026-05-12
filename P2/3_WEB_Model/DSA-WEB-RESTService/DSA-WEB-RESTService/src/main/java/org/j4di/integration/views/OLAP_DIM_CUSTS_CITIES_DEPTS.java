package org.j4di.integration.views;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

@Getter
@Entity
@Immutable
@Table(name = "OLAP_DIM_CUSTS_CITIES_DEPTS")
public class OLAP_DIM_CUSTS_CITIES_DEPTS {
    @Id
    private Long customerId;
    private String customerName;
    private Long idCity;
    private String cityName;
    private Long idDepartament;
    private String departamentName;
}