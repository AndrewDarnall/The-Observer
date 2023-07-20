package com.drew.camel_simple;

import org.apache.camel.builder.RouteBuilder;
import org.springframework.stereotype.Component;


@Component
public class FileMoverRoute extends RouteBuilder{
    
    @Override
    public void configure() throws Exception {

        from("stream:file?fileName=/datastorage/mastodon_data_.jsonl&scanStream=true&scanStreamDelay=1000")
        .log("${headers}")
        .log("${body}")
        // Redirect to Kafka ~ careful with the Kafka routing
        .to("kafka:dataflow?brokers=10.0.100.23:9092");
        // Debug only
        // .to("stream:file?fileName=/processed/out.jsonl");        

    }


}

/**
 * Basically Camel uses specifi components to read from specific sources
 * It might be more verbose than other software but I still think that it gives
 * the right ammount of flexibility and integration with other frameworks such as spring
 * or other software such as Apache Kafka
 * 
 * The overridden method, configure, is the heart of the routing performaed by Camel
 */