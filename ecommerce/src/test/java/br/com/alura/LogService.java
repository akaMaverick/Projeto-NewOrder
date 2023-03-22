package br.com.alura;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class LogService {
    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Pattern.compile("ECOMMERCE.*"));

        while(true) {
        var records = consumer.poll(Duration.ofMillis(100));
        if(!records.isEmpty()) {
            System.out.println("Encontrei " + records.count() + " registros.");
            for(var record : records) {
                System.out.println("----------------------------------------------------------------");
                System.out.println("LOG " + record.topic());
                System.out.println("Essa é a chave: " + record.key());
                System.out.println("Esse é o valor: " + record.value());
                System.out.println("Essa é a partição: " + record.partition());
                System.out.println("Esse é o offset:" + record.offset());
    
            }
        }
        
    }
    }

    private static Properties properties() { 
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, LogService.class.getSimpleName());
        return properties;
    }
}
