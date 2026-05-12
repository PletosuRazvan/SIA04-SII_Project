package org.datasource.jpa.views.product;

import org.datasource.jpa.JPADataSourceConnector;
import org.springframework.stereotype.Service;

import jakarta.persistence.*;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
@Service
public class ProductViewBuilder {
	private static Logger logger = Logger.getLogger(ProductViewBuilder.class.getName());

	protected String JPQL_PRODUCTS_SELECT =
			"SELECT NEW org.datasource.jpa.views.product.ProductView("
			+ "p.productCode, p.basePrice, p.prodCategory, p.prodName) "
			+ "FROM ProductView p";

	protected List<ProductView> productViewList = new ArrayList<>();

	public List<ProductView> getProductViewList() {
		return productViewList;
	}

	public ProductViewBuilder build(){
		return this.select();
	}

	protected ProductViewBuilder select(){
		EntityManager em = dataSourceConnector.getEntityManager();
		Query viewQuery = em.createQuery(JPQL_PRODUCTS_SELECT);
		//Query viewQuery = em.createNamedQuery("ProductView.findAll");
		//viewQuery.setFirstResult()
		//viewQuery.setMaxResults()
		this.productViewList = viewQuery.getResultList();

		return this;
	}

	//
	protected JPADataSourceConnector dataSourceConnector;

	public ProductViewBuilder(JPADataSourceConnector dataSourceConnector) {
		super();
		this.dataSourceConnector = dataSourceConnector;
	}
	
}