package com.phenom.lightsaber.dataprocessor;

import com.phenom.lightsaber.kafkamanagers.KafkaEventProducer;
import com.phenom.lightsaber.routers.Router;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class ProcessData {

    final Logger log = LoggerFactory.getLogger(ProcessData.class);

    @Autowired
    KafkaEventProducer kafkaEventProducer;

    public JSONObject processRawEvent(JSONObject rawEvent, boolean batchKafka) {
        if (!rawEvent.has("timestamp")) {
            Date date = new Date();
            SimpleDateFormat formatter = new SimpleDateFormat("MM/dd/yy hh:mm:ss aa");
            String strDate = formatter.format(date);
            rawEvent.put("timestamp", strDate);
        }
        rawEvent.put("ls_timestamp", rawEvent.getString("timestamp"));

        String clientToken = "nullToken";

        if (rawEvent.has("clientToken"))
            clientToken = rawEvent.getString("clientToken");
        String batch = batchKafka ? "Batch_" : "";
        String topic = "Phenom_Track_" + batch + clientToken + "_TOPIC";
//        return kafkaEventProducer.sendMessage(rawEvent.toString(), topic, batchKafka).map(d -> {
//            System.out.println(d);
//            return d;
//        });
        return kafkaEventProducer.sendMessage(rawEvent.toString(), topic, batchKafka);
    }
}
