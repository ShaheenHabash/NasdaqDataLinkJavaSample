/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nasdaqdatalink;

import com.nasdaq.ncdsclient.NCDSClient;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.log4j.xml.DOMConfigurator;

/**
 *
 * @author DELL
 */
public class NasdaqDataLink {

    /**
     * @param args the command line arguments
     */
    //*************************************************************************
    public static void main(String[] args) throws ParseException {
        //Get the timeStamp for this day at 03:00AM EST time
        TimeZone xCurrentTimeZone = TimeZone.getDefault();
        long xCurrentTimeZoneDifference = xCurrentTimeZone.getRawOffset();
        DateFormat xDateFormatDate = new SimpleDateFormat("yyyy-MM-dd");
        Date xDate = new Date();
        String xDateNow = xDateFormatDate.format(xDate);
        SimpleDateFormat xDateFormatDateTime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date xDateTime = xDateFormatDateTime.parse(xDateNow + " 08:00:00");
        long xDiffInMillies = xDate.getTime() - xDateTime.getTime();
        long xCurrentTStamp = System.currentTimeMillis();
        long xReqTStamp = xCurrentTStamp - xDiffInMillies + xCurrentTimeZoneDifference;
        xReqTStamp = xReqTStamp - (xReqTStamp % 1000);

        try {
            //Load Log4j config file
            String xLog4jConfigPath = System.getProperty("user.dir") + "\\log4j.xml";
            DOMConfigurator.configure(xLog4jConfigPath);

            //Load clientAuthentication-config properties file
            Properties securityCfg = new Properties();
            securityCfg.setProperty("oauth.token.endpoint.uri", "https://clouddataservice.auth.nasdaq.com/auth/realms/pro-realm/protocol/openid-connect/token");
            securityCfg.setProperty("oauth.client.id","awaed-jt-chen");
            securityCfg.setProperty("oauth.client.secret", "n8WWiujsn6y0FLEiH2J9XZiIqLEEJSDi");

            //Load kafka-config properties file
            Properties kafkaCfg = new Properties();
            kafkaCfg.setProperty("bootstrap.servers", "http://kafka-bootstrap.clouddataservice.nasdaq.com:9094");
            kafkaCfg.setProperty("request.timeout.ms", "10000");
            kafkaCfg.setProperty("retry.backoff.ms", "500");
            kafkaCfg.setProperty("fetch.max.wait.ms", "30");
            kafkaCfg.setProperty("fetch.min.bytes", "131072");
            kafkaCfg.setProperty("fetch.max.bytes","8388608");
            kafkaCfg.setProperty("max.partition.fetch.bytes","8388608");
            kafkaCfg.setProperty("enable.auto.commit","true");
            kafkaCfg.setProperty("auto.commit.interval.ms","1000");
            kafkaCfg.setProperty("isolation.level", "read_committed");
            kafkaCfg.setProperty("session.timeout.ms", "10000");
            kafkaCfg.setProperty("heartbeat.interval.ms", "3000");
            kafkaCfg.setProperty("auto.offset.reset", "latest");
            kafkaCfg.setProperty("max.poll.interval.ms", "30000");
            kafkaCfg.setProperty("max.poll.records", "50000");

            // TODO code application logic here
            NCDSClient ncdsClient = new NCDSClient(securityCfg, kafkaCfg);

            String topic = "TOTALVIEW";
            Consumer consumer = ncdsClient.NCDSKafkaConsumer(topic, xReqTStamp);
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofMinutes(Integer.parseInt("1")));
                if (records.count() == 0) {
                    System.out.println(" No Records Found for the Topic:" + topic);
                }
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.println(" value :" + record.value().toString());
                }
            }
        } catch (Exception xEx) {
            Logger.getLogger(NasdaqDataLink.class.getName()).log(Level.SEVERE, null, xEx);
        }
    }
    //*************************************************************************
}
