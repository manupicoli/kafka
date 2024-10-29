package org.example;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try(var dispatcher = new KafkaDispatcher()) {
            for (var i = 0; i < 10; i++) {
                var key = UUID.randomUUID().toString();
                var value = key + ",898902,2392839289";
                dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

                var email = "Thank you for your order! We are already processing it for you.";
                dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
            }
        }
    }
}