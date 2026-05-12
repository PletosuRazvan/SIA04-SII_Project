package org.j4di.access.views.invoices;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import org.hibernate.annotations.Immutable;

import java.time.LocalDate;

@Getter
@Entity
@Immutable
@Table(name="INVOICES_VIEW")
public class INVOICES_VIEW {
    @Id
    private Long invoiceId;
    private Long customerId;
    private String customerName;
    private LocalDate invoiceDate;
}
