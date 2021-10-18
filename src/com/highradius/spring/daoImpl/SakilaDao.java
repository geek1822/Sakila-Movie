package com.highradius.spring.daoImpl;

import java.sql.*;
import java.util.*;
import org.hibernate.Criteria;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.criterion.Order;
import org.hibernate.criterion.Restrictions;

import com.highradius.spring.dao.DaoInterface;
import com.highradius.spring.model.FilmPojo;
import com.highradius.spring.model.LanguagePojo;

public class SakilaDao implements DaoInterface{

	// Hibernate Session Variables Information
		private SessionFactory factory = null;
		private Session session = null;
		private Transaction transaction = null;
		private String hql = "";
		private Query query = null;
		private Criteria criteria = null;
		private Configuration config = null;
		
		// Response and Helper Variables
		private List<FilmPojo> list = new ArrayList<>();
		private List<FilmPojo> arr = new ArrayList<>();
		private List<LanguagePojo> langList = new ArrayList<LanguagePojo>();
		private boolean success = true;
		private HashMap<String, Object> responseData = new HashMap<>();
		
		// Session Factory Method
		@SuppressWarnings("deprecation")
		public SessionFactory getSession() {

			try {
				//Creating configuration object -> The first hibernate object we create
				 this.config = new Configuration();
				 this.config.configure("hibernate.cfg.xml");
				 this.factory = this.config.buildSessionFactory();  
			
				} catch(Exception e) {
					
					e.printStackTrace();
					
				}
			
			return this.factory;
			
		}
		
		// Helper Method to Get Language Name
		public Integer getLanguageId(String languageName) {
			
			
			Session sessionLang = getSession().openSession();
			
			// // Running HQL Query
			
			Query queryLang = sessionLang.createQuery("SELECT language_id FROM LanguagePojo WHERE name = '" + languageName + "'");

			return (Integer) queryLang.uniqueResult();
			
		}

		// Helper Method to Get Total Number of Rows
		public Long getTotalRows() {
			
			Session sessionRows = getSession().openSession();
			Query queryRows = sessionRows.createQuery("SELECT COUNT(*) FROM FilmPojo WHERE isDeleted != 1");
		
			return (Long) queryRows.uniqueResult();
				
		}
			
	    /* ####################################################################################
		#                           `getData` Execute Function                                #
		#################################################################################### */
		@SuppressWarnings({ "unchecked" })
		public HashMap<String, Object> getSakilaData(Integer start, Integer limit) {
			
		
			// Hibernate Connectivity
			
			this.session = this.getSession().openSession();
			
			// Acquiring Data From Object using ORM and Adding Criterias [Filter] to our Result
			try {
				
				// Executing Query and Adding Filters [Criteria]
				this.criteria = this.session.createCriteria(FilmPojo.class);
				this.criteria.add(Restrictions.eq("isDeleted", false)); //isDELETED == 0
				this.criteria.setFirstResult(start);
				this.criteria.setMaxResults(limit);
				this.criteria.addOrder(Order.asc("film_id"));
				this.list = this.criteria.list();
				
				// Mending the Results according to our Requirements
				for (Iterator<FilmPojo> iterator = list.iterator(); iterator.hasNext(); ) {
					
					FilmPojo filmObj = (FilmPojo) iterator.next();
					FilmPojo  acutalObj = new FilmPojo();
					acutalObj.setFilm_id(filmObj.getFilm_id());
					acutalObj.setDescription(filmObj.getDescription());
					acutalObj.setDirector(filmObj.getDirector());
					acutalObj.setLanguage_name(filmObj.getLanguage().getName());
					acutalObj.setRating(filmObj.getRating());
					acutalObj.setRelease_year(filmObj.getRelease_year());
					acutalObj.setTitle(filmObj.getTitle());
					acutalObj.setSpecial_features(filmObj.getSpecial_features());
					arr.add(acutalObj);
					
				 }
				
			} catch(Exception e) {
				
				e.printStackTrace();
				this.responseData.put("success", false);
				return this.responseData;
				
			} finally {
				
				// Closing the Transaction Session
				this.session.close();
				
			}
			
			// Converting the HashMap into Response
			this.responseData.put("success", true);
			this.responseData.put("totalCount", this.getTotalRows());
			this.responseData.put("filmData", this.arr);
			
			return this.responseData;
			
		}
		
	    /* ####################################################################################
		#                           `geLangData` Execute Function                             #
		#################################################################################### */
		@SuppressWarnings("unchecked")
		public HashMap<String, Object> getSakilaLangData() {
		
			
			// Hibernate Connectivity
		
			this.session = this.getSession().openSession();
			
			// Acquiring Data From Object using ORM and Adding Criterias [Filter] to our Result
			try {
				
				this.criteria = this.session.createCriteria(LanguagePojo.class);
				this.langList = this.criteria.list();
				
			} catch(Exception e) {
				
				e.printStackTrace();
				this.responseData.put("success", false);
				return this.responseData;
				
			} finally {
				
				// Closing the Transaction Session
				this.session.close();
				
			}

			// Converting the HashMap into Response
			this.responseData.put("success", true);
			this.responseData.put("totalCount", this.langList.size());
			this.responseData.put("langData", this.langList);
			
					
			return this.responseData;
			
		}
		
		/* ####################################################################################
		#                           `addData` Execute Function                                #
		#################################################################################### */
		
		public HashMap<String, Object> addSakilaData(FilmPojo obj) {
			
			
			// Hibernate Connectivity
			this.session = this.getSession().openSession();
			
			// ORM Operations
			try {
				
				// Acquiring Language ID from Language Model Object
				LanguagePojo langObj = (LanguagePojo) this.session.load(LanguagePojo.class, this.getLanguageId(obj.getLanguage_name())); 
				obj.setLanguage(langObj);
				
				// Executing HQL Query
				if(!this.session.getTransaction().isActive()) 
					this.session.beginTransaction();
			
				this.session.save(obj);
				
				// Commiting the Transaction
				this.session.getTransaction().commit();
				System.out.println("Query Sucessful! Inserted 1 Row in DB.");
				success = true;
				
			} catch(Exception e) {
				
				e.printStackTrace();
				this.success = false;
				
			} finally {
				
				// Closing the Transaction Session
				this.session.close();
				
			}
			
			// Converting the HashMap into Response
			this.responseData.put("success", this.success);
			
			return this.responseData;

		}

		/* ####################################################################################
		#                          `editData` Execute Function                                #
		#################################################################################### */

		public HashMap<String, Object> editSakilaData(FilmPojo obj) {
			
			
			 
			// Hibernate Connectivity
			this.session = this.getSession().openSession();
			
			// ORM Operations
			try {
				
				// Acquiring Language ID from Language Model Object
				LanguagePojo langObj = (LanguagePojo) session.load(LanguagePojo.class, getLanguageId(obj.getLanguage_name())); 
				obj.setLanguage(langObj);
				
				// Executing HQL Query
				if(!this.session.getTransaction().isActive()) 
					this.session.beginTransaction();
				this.session.saveOrUpdate(obj);
				
				// Commiting the Transaction
				this.session.getTransaction().commit();
				System.out.println("Query Sucessful! Updated 1 Row in DB.");
				this.success = true;
				
			} catch(Exception e) {
				
				e.printStackTrace();
				this.success = false;
				
			} finally {
				
				// Closing the Transaction Session
				this.session.close();
				
			}
			
			// Converting the HashMap into Response
			this.responseData.put("success", this.success);
			
			return this.responseData;

		}

		/* ####################################################################################
		#                         `deleteData` Execute Function                               #
		#################################################################################### */
		
		public HashMap<String, Object> deleteSakilaData(String del_filmIds) {
		
			// Hibernate Connectivity
			this.session = this.getSession().openSession();
			
			// Making the Request into Suitable Format
			String[] filmIdListString = del_filmIds.split(",");
			ArrayList<Integer> filmIdList = new ArrayList<>();
			for(String id : filmIdListString) {
				filmIdList.add(Integer.parseInt(id));
			}
			
			// ORM Operations
			try {
				
				// Opening up Transaction Session
				if(!this.session.getTransaction().isActive()) 
					this.transaction = this.session.beginTransaction();
				
				// Using For Loop to Perform the Task
				for(int id : filmIdList) {
								
					// HQL Query String Generation
					//overriding Hibernate’s default remove operation inorder to perform soft delete
					this.hql  = "UPDATE FilmPojo SET isDeleted = 1 WHERE film_id = :filmid";
					this.query = this.session.createQuery(hql);
					this.query.setInteger("filmid", id);
					
					// Execute HQL Query
					this.query.executeUpdate();

				}
				
				// Commiting the Transaction
				this.transaction.commit();
						
				
			} catch (Exception e) {
				
				if (this.transaction != null) 
					this.transaction.rollback();
				e.printStackTrace(); 
				this.success = false;
				
			} finally {
				
				// Closing the Transaction Session
				this.session.close();
				
			}
			
			// Converting the HashMap into Response
			this.responseData.put("success", this.success);
			
			return this.responseData;

		}
}