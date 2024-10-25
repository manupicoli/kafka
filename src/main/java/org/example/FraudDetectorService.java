package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class FraudDetectorService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("ECOMMERCE_NEW_ORDER"));

        //para que esse serviço continue ouvindo as novas mensagens, colocamos dentro de um laço
        while(true){
            var records = consumer.poll(Duration.ofMillis(100));
            if(!records.isEmpty()){
                System.out.println(records.count() + " records found");

                for(var record : records){
                    System.out.println("Processing new order, checking for fraud");
                    System.out.println(record.key() + " / " + record.value() + " / " + record.partition() + " / " + record.offset());
                    try{
                        Thread.sleep(5000);
                    } catch (InterruptedException e){
                        e.printStackTrace();
                    }
                    System.out.println("Order processed");
                    System.out.println("-----------------------------------------");
                }
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //determinamos um grupo para cada consumidor, para ter certeza que ele vai receber todas as mensagens
        //quando dois consumidores tem o mesmo grupo, essas mensagens serão distribuidas entre eles
        //ou seja, processa em paralelo as mensagens, mas não sabemos qual consome qual
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, FraudDetectorService.class.getSimpleName());
        return properties;
    }
}
