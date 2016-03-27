/* Copyright (c) 2009 & onwards. MapR Tech, Inc., All rights reserved */
package solution;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class MyProducer {

    // Set the number of messages to send.
    public static int numMessages = 60;
    // Declare a new producer
    public static KafkaProducer producer;

    public static void main(String[] args) throws IOException {
        // Set the stream and topic to publish to.
        String topic = "/user/user01/pump:sensor";
        if (args.length == 1) {
            topic = args[0];
        }

        configureProducer();
        File f = new File("./data/sensordata.csv");
        FileReader fr = new FileReader(f);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();
        while (line != null) {
            String[] temp = line.split(",");
            String key=temp[0];
            /* Add each message to a record. A ProducerRecord object
             identifies the topic or specific partition to publish
             a message to. */
            ProducerRecord<String, String> rec = new ProducerRecord<String, String>(topic,key, line);

            // Send the record to the producer client library.
            System.out.println("Sending to topic " + topic);
            producer.send(rec);
            System.out.println("Sent message " + line);
            line = reader.readLine();

        }

        producer.close();
        System.out.println("All done.");

        System.exit(1);

    }

    /* Set the value for a configuration parameter.
     This configuration parameter specifies which class
     to use to serialize the value of each message.*/
    public static void configureProducer() {
        Properties props = new Properties();
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }

}
