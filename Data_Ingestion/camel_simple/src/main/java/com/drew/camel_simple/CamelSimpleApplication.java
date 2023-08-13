package com.drew.camel_simple;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * ===============================================================================
 * 	CamelSimpleApplication is the main entrypoint to the Data Ingestor component
 * 	of the TAP project, I used the Springboot framework due to its plugin rich
 *  echosystem and the seamless integration that it has with working with data 
 * 	sources and data related technologies such as Apache Camel
 * ===============================================================================
 */
@SpringBootApplication
public class CamelSimpleApplication {

	/**
	 * The main method launches the Spring applications with an instance of the
	 * camel class, which is declared in another source file
	 */
	public static void main(String[] args) {
		SpringApplication.run(CamelSimpleApplication.class, args);
	}

}
