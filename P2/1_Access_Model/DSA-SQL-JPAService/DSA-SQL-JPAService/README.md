# IIS

## SQL REST Service: JPA-DATA-SOURCE-SERVICE-WRAPPER

### JDBC Spring Boot Service
* Jakarta Persistence - Provider: Hibernate from [spring-boot-starter-data-jpa]
* SpringBoot 3

### SpringBoot service desc
* SBT jar: DSA-SQL-JPAService-2026.1.jar

## Spring Boot Container Doc
* [Docker for SBT](https://spring.io/guides/gs/spring-boot-docker)
  
## DevOp Local Flow
* Image name: iis-dsa/DSA-SQL-JPAService  
mvn package
docker build --build-arg JAR_NAME="DSA-SQL-JPAService-2026.1.jar" -t iis-dsa/DSA-SQL-JPAService .
docker run --name=DSA-SQL-JPAService -p 8090:8090 -d iis-dsa/DSA-SQL-JPAService  
docker start jdbc-data-source-service-wrapper 

## Clean-up docker
cd DSA-SQL-JPAService
docker stop DSA-SQL-JPAService
docker rm DSA-SQL-JPAService
docker rmi iis-dsa/DSA-SQL-JPAService
