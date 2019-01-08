package io.schneezey.producer;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.schneezey.avro.reflect.PojoSerDes;
import io.schneezey.pojo.TestPojo;
import io.schneezey.pojo.TestSub2Pojo;
import io.schneezey.pojo.TestSubPojo;

/**
 * Hello world!
 *
 */
public class Producer 
{
	static PojoSerDes<TestPojo> mapper = new PojoSerDes<TestPojo>();
	
    public static void main( String[] args ) {        
    	produceCustomMappedPojo();
    }

    private static TestPojo createTestPojo() {
    	Calendar cal = Calendar.getInstance();
    	TestPojo pojo = new TestPojo();
    	
    	pojo.setId(Random.randomInt(999999999));
	
    	pojo.setTestPBoolean(Random.randomBoolean());
    	pojo.setTestPBytes(Random.randomByte(Random.randomInt(128)));
    	cal.set(1910 + Random.randomInt(85), Random.randomInt(12), Random.randomInt(28));
    	pojo.setTestDate(cal.getTime());
    	pojo.setTestPDouble(Random.randomDouble());
    	pojo.setTestPFloat(Random.randomFloat());
    	pojo.setTestPInt(Random.randomInt());
    	pojo.setTestPLong(Random.randomLong());
    	pojo.setTestPString(Random.randomWord(40));

    	pojo.setTestN1Boolean(Random.randomBoolean());
    	pojo.setTestNBytes(Random.randomByte(Random.randomInt(128)));
    	cal.set(1910 + Random.randomInt(85), Random.randomInt(12), Random.randomInt(28));
    	pojo.setTestN1Date(cal.getTime());
    	pojo.setTestN1Double(Random.randomDouble());
    	pojo.setTestN1Float(Random.randomFloat());
    	pojo.setTestN1Int(Random.randomInt());
    	pojo.setTestN1Long(Random.randomLong());

		int value = Random.randomInt(3);
		switch (value) {
		case 1:
			pojo.setTestenum(TestPojo.TEST_ENUM.TYPEA);
			break;
		case 2:
			pojo.setTestenum(TestPojo.TEST_ENUM.TYPEB);
			break;
		case 3:
		default:
			pojo.setTestenum(TestPojo.TEST_ENUM.TYPEC);
			break;
		}
		
		ArrayList<TestSubPojo> subPojos = new ArrayList<TestSubPojo>();
		int limit = Random.randomInt(5);
		for (int ii = 0; ii < limit; ii++) {
			TestSubPojo subpojo = new TestSubPojo();
			subpojo.setSubPBoolean(Random.randomBoolean());
			subpojo.setSubPBytes(Random.randomByte(Random.randomInt(40)));
			subpojo.setSubPDouble(Random.randomDouble());
			subpojo.setSubPFloat(Random.randomFloat());
			subpojo.setSubPInt(Random.randomInt());
			subpojo.setSubPLong(Random.randomLong());
			
			ArrayList<TestSub2Pojo> subjos = new ArrayList<TestSub2Pojo>();
			int sublimit = Random.randomInt(3);
			for (int jj = 0; jj < sublimit; jj++) {
				TestSub2Pojo subjo = new TestSub2Pojo();
				subjo.setSubPBoolean(Random.randomBoolean());
				subjo.setSubPBytes(Random.randomByte(Random.randomInt(40)));
				subjo.setSubPDouble(Random.randomDouble());
				subjo.setSubPFloat(Random.randomFloat());
				subjo.setSubPInt(Random.randomInt());
				subjo.setSubPLong(Random.randomLong());
				
				subjos.add(subjo);
			}
			subpojo.setSubjos(subjos);
			subPojos.add(subpojo);
		}
		pojo.setSubPojos(subPojos);

    	return pojo;
    }
    
	private static void produceCustomMappedPojo() {
		KafkaProducer<String,Object> producer = null;
    	try {
	        Properties config = new Properties();
	        config.put("client.id", InetAddress.getLocalHost().getHostName());
	        config.put("bootstrap.servers", "localhost:9092");
	        config.put("acks", "all");
	        
	        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
	                org.apache.kafka.common.serialization.StringSerializer.class);
	        
	        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
	        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

	        producer = new KafkaProducer<String, Object> (config);
	        
	        
	        for (int i = 0; i < 3; i++) {
	        	TestPojo pojo = createTestPojo();
		        ProducerRecord<String,Object> avroRecord = new ProducerRecord<String,Object> ("test.pojo.stream.avro", pojo.getId().toString(), mapper.serializePojo(pojo));
	        	producer.send( avroRecord );
		        System.out.println("Avro Message produced" + pojo.toString());
			}
	        producer.close();
    	} catch (Exception ee) {
    		ee.printStackTrace();
    		System.out.println("Exception Caught \n" + ee.getLocalizedMessage());
    	}
    	finally {
    		if (producer != null) producer.close();
    	}
	}

}
