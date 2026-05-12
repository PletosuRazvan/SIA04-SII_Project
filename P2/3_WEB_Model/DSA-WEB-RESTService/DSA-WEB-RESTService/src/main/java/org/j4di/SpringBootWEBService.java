package org.j4di;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

import java.util.logging.Logger;
/*
 * mvn spring-boot:run
 */
@SpringBootApplication
public class SpringBootWEBService
		extends SpringBootServletInitializer
{
	private static Logger logger = Logger.getLogger(SpringBootWEBService.class.getName());
	
	public static void main(String[] args) {
		logger.info("Loading ... SpringBootWEB - SparkSQLService Default Settings ... JPA");
		SpringApplication.run(SpringBootWEBService.class, args);
	}
}

/* <!-- Spark-Hive JDBC: Full configuration: hive-jdbc complete dependency set  -->
		<hive-jdbc.version>4.2.0</hive-jdbc.version>

		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-jdbc</artifactId>
			<version>${hive-jdbc.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hadoop</groupId>
			<artifactId>hadoop-common</artifactId>
			<version>3.4.0</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-common</artifactId>
			<version>${hive-jdbc.version}</version>
		</dependency>
		<!-- The service components (Service + RPC) with a fix for SBT-Hive incompatibility -->
		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-service</artifactId>
			<version>${hive-jdbc.version}</version>
			<exclusions>
				<exclusion>
					<groupId>net.shibboleth.tool</groupId>
					<artifactId>xmlsectool</artifactId>
				</exclusion>
				<exclusion>
					<groupId>org.eclipse.jetty</groupId>
					<artifactId>apache-jsp</artifactId>
				</exclusion>
			</exclusions>
		</dependency>
		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-service-rpc</artifactId>
			<version>${hive-jdbc.version}</version>
		</dependency>

*/
/* <!-- Spark-Hive JDBC  alt minimized configuration-->
		<hive-jdbc.version>4.2.0</hive-jdbc.version>
		<spark.version>4.1.1</spark.version>
		<scala.binary.version>2.13</scala.binary.version>

		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-jdbc</artifactId>
			<version>${hive-jdbc.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-common</artifactId>
			<version>${hive-jdbc.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.spark</groupId>
			<artifactId>spark-hive-thriftserver_${scala.binary.version}</artifactId>
			<version>${spark.version}</version>
		</dependency>
	</dependencies>

Problems fixed:
import org.apache.thrift.protocol.TProtocol;
import org.apache.hive.service.cli.HiveSQLException;
 */
/* <!-- Spark-Hive JDBC Second minimal configuraion -->
<hive-jdbc.version>4.2.0</hive-jdbc.version>

		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-jdbc</artifactId>
			<version>${hive-jdbc.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-cli</artifactId>
			<version>${hive-jdbc.version}</version>
			<exclusions>
				<exclusion>
					<groupId>org.eclipse.jetty</groupId>
					<artifactId>apache-jsp</artifactId>
				</exclusion>
			</exclusions>
		</dependency>

 */
/* <!-- Spark-Hive JDBC Minimized configuration-->
		<hive-jdbc.version>4.2.0</hive-jdbc.version>

		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-jdbc</artifactId>
			<version>${hive-jdbc.version}</version>
		</dependency>
		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-service</artifactId>
			<version>${hive-jdbc.version}</version>
			<exclusions>
				<exclusion>
					<groupId>org.eclipse.jetty</groupId>
					<artifactId>apache-jsp</artifactId>
				</exclusion>
			</exclusions>
		</dependency>
*/
/* <!-- Spark-Hive JDBC - most simplified -->
<hive-jdbc.version>4.2.0</hive-jdbc.version>

		<dependency>
			<groupId>org.apache.hive</groupId>
			<artifactId>hive-jdbc</artifactId>
			<classifier>standalone</classifier>
			<version>${hive-jdbc.version}</version>
			<exclusions>
				<exclusion>
					<groupId>org.slf4j</groupId>
					<artifactId>slf4j-api</artifactId>
				</exclusion>
			</exclusions>
		</dependency>
 */
/*
JPQL Pagination
https://www.baeldung.com/jpa-pagination
 */

