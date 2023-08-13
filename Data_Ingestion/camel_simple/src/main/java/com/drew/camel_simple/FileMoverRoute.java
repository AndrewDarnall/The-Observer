package com.drew.camel_simple;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;

/**
 * ====================================================================
 *  This annotated component which extends the RouteBuilder class
 *  is the heart of the routing logic of the data ingestor
 * 
 *  Apache Camel is much more sophisticated compared to what is shown
 *  in this source file, however I chose it over other technologies
 *  due to the flexibility and integration that it provides
 * ====================================================================
 */
@Component
public class FileMoverRoute extends RouteBuilder{
    
    /**
     * All route creations follow the same syntax as the configure() method
     * the first element to specify in the method chain is the source of
     * the data and the source type, the specific type of data source
     * has to be included as a dependency in the project
     * 
     * Some other pre-processing steps can be added, much like a middle ware
     * in JavaScript, this comes in handy when processing certain information
     * that might need some pre-processing such as credit card details
     * 
     * The final step is to specify the 'output sink', specifying in the
     * destination type for which yet another connector dependency might
     * need to be installed, and the 'real destination' or the IP/URL of
     * the destination endpoint
     * 
     * In this case the destination is a kafka broker with a specific topic
     * to write to and a specific IP address for it
     * 
     * This code has been designed to be run indipendently on separate containers
     * in order to:
     * 
     *  1) be scaled easily
     *  2) be extended/maintained better
     * 
     */
    @Override
    public void configure() throws Exception {

        String dataSource = System.getenv("DATA_SOURCE");

        from("stream:file?fileName=/data/" + dataSource + ".jsonl&scanStream=true&scanStreamDelay=1000")
        .log("${headers}")
        .log("${body}")
        .to("kafka:" + dataSource + "?brokers=10.0.100.23:9092");
        // Debug only
        // .to("stream:file?fileName=/processed/out.jsonl");        

    }


}