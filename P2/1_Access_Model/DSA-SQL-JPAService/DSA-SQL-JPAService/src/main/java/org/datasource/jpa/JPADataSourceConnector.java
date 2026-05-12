package org.datasource.jpa;

import org.springframework.stereotype.Service;
import org.springframework.web.context.annotation.ApplicationScope;

import jakarta.persistence.*;

@Service @ApplicationScope
public class JPADataSourceConnector {
	@PersistenceContext
	private EntityManager em;

	public EntityManager getEntityManager(){
		return em;
	}
	
}