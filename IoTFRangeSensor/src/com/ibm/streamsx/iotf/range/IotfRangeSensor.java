package com.ibm.streamsx.iotf.range;

import static quarks.analytics.math3.stat.Statistic.MAX;
import static quarks.analytics.math3.stat.Statistic.MEAN;
import static quarks.analytics.math3.stat.Statistic.MIN;
import static quarks.analytics.math3.stat.Statistic.STDDEV;

import java.io.File;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.pi4j.io.gpio.Pin;
import com.pi4j.io.gpio.RaspiPin;

import quarks.analytics.math3.json.JsonAnalytics;
import quarks.connectors.iot.IotDevice;
import quarks.connectors.iot.QoS;
import quarks.connectors.iotf.IotfDevice;
import quarks.function.Supplier;
import quarks.providers.direct.DirectProvider;
import quarks.providers.direct.DirectTopology;
import quarks.topology.TStream;
import quarks.topology.TWindow;

public class IotfRangeSensor {
    private static final Pin echoPin = RaspiPin.GPIO_05; // PI4J custom numbering (pin 18 on RPi2)
    private static final Pin trigPin = RaspiPin.GPIO_04; // PI4J custom numbering (pin 16 on RPi2)
    private static final Pin ledPin = RaspiPin.GPIO_01;
    
	public static void main(String[] args) {
		
		 	if(args.length != 2)
		    {
		        System.out.println("Proper Usage is:\n   "
		        		+ "   java program device.cfg simulated_boolean \n"
		        		+ "Example: \n"
		        		+ "   java -cp $QUARKS/target/java8/samples/lib/'*':$PI4J_LIB/'*':bin/ com.ibm.streamsx.iotf.range.IotfRangeSensor device.cfg false");
		        System.exit(0);
		    }
	        
	        String deviceCfg = args[0];
	        Boolean simulated = Boolean.parseBoolean(args[1]);
	        
	        DirectProvider tp = new DirectProvider();
	        DirectTopology topology = tp.newTopology("IotfSensors");
	
	        // Declare a connection to IoTF
	        IotDevice device = new IotfDevice(topology, new File(deviceCfg));
	
	        // HC-SR04 Range sensor for this device.
	        rangeSensor(device, simulated, true);
	        
	        // In addition create a heart beat event to
	        // ensure there is some immediate output and
	        // the connection to IoTF happens as soon as possible.
	        TStream<Date> hb = topology.poll(() -> new Date(), 1, TimeUnit.MINUTES);
	        // Convert to JSON
	        TStream<JsonObject> hbj = hb.map(d -> {
	            JsonObject j = new  JsonObject();
	            j.addProperty("when", d.toString());
	            j.addProperty("hearbeat", d.getTime());
	            return j;
	        });
	        hbj.print();
	        device.events(hbj, "heartbeat", QoS.FIRE_AND_FORGET);
	
	        // Subscribe to commands of id "display" for this
	        // device and print them to standard out
	        TStream<String> statusMsgs = displayMessages(device);
	        statusMsgs.print();
	        if (!simulated){
	        	LED led = new LED(ledPin);
	        	statusMsgs.sink(j -> led.flash(1000));
	        }
	
	        tp.submit(topology);
	    }

    /**
     * Connect to an HC-SR04 Range Sensor
     * 
     * @param device
     *            IoTF device
     * @param print
     *            True if the data submitted as events should also be printed to
     *            standard out.
     */
    public static void rangeSensor(IotDevice device, boolean simulated, boolean print) {
    	
		Supplier<Double> sensor;
		
		if (simulated){
			sensor = new SimulatedRangeSensor(); 
		} else {
			sensor = new RangeSensor(echoPin, trigPin);
		}
		
		TStream<Double> distanceReadings = device.topology().poll(sensor, 1, TimeUnit.SECONDS);
		distanceReadings.print();
		
		//filter out bad reading that are out of the sensor's 4m range
		distanceReadings = distanceReadings.filter(j -> j < 400.0);
		
		TStream<JsonObject> sensorJSON = distanceReadings.map(v -> {
			JsonObject j = new JsonObject();
			j.addProperty("name", "rangeSensor");
			j.addProperty("reading", v);
			return j;
		});
        
        // Create a window on the stream of the last 50 readings partitioned
        // by sensor name. In this case two independent windows are created (for a and b)
        TWindow<JsonObject,JsonElement> sensorWindow = sensorJSON.last(20, j -> j.get("name"));
        
        // Aggregate the windows calculating the min, max, mean and standard deviation
        // across each window independently.
        sensorJSON = JsonAnalytics.aggregate(sensorWindow, "name", "reading", MIN, MAX, MEAN, STDDEV);
        
        // Filter so that only when the sensor is beyond 2.0 (absolute) is a reading sent.
        sensorJSON = sensorJSON.filter(j -> Math.abs(j.get("reading").getAsJsonObject().get("MEAN").getAsDouble()) < 30.0);
        
        if (print)
        	sensorJSON.print();

        // Send the device streams as IoTF device events
        // with event identifier "sensors".
        device.events(sensorJSON, "sensors", QoS.FIRE_AND_FORGET);
    }

    /**
     * Subscribe to IoTF device commands with identifier {@code display}.
     * Subscribing to device commands returns a stream of JSON objects that
     * include a timestamp ({@code tsms}), command identifier ({@code command})
     * and payload ({@code payload}). Payload is the application specific
     * portion of the command. <BR>
     * In this case the payload is expected to be a JSON object containing a
     * {@code msg} key with a string display message. <BR>
     * The returned stream consists of the display message string extracted from
     * the JSON payload.
     * <P>
     * Note to receive commands a analytic application must exist that generates
     * them through IBM Watson IoT Platform.
     * </P>
     * 
     * @see IotDevice#commands(String...)
     */
    public static TStream<String> displayMessages(IotDevice device) {
        // Subscribe to commands of id "status" for this device
        TStream<JsonObject> statusMsgs = device.commands("display");

        // The returned JSON object includes several fields
        // tsms - Timestamp in milliseconds (this is generic to a command)
        // payload.msg - Status message (this is specific to this application)

        // Map to a String object containing the message
        return statusMsgs.map(j -> j.getAsJsonObject("payload").getAsJsonPrimitive("msg").getAsString());
    }
}
