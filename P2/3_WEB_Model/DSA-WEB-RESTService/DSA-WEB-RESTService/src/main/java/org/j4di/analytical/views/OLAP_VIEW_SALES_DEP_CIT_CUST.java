package org.j4di.analytical.views;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

import java.time.LocalDate;

@Getter
@Entity
@Immutable
@Table(name = "OLAP_VIEW_SALES_DEP_CIT_CUST")
public class OLAP_VIEW_SALES_DEP_CIT_CUST {
    @Id
    private String customerName;
    private String departamentName;
    private String cityName;
    private Long sales_amount;
}