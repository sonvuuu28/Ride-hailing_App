// =================== FLINK CORE ===================
import org.apache.flink.api.common.functions.MapFunction;       // Map transformation on each event
import org.apache.flink.api.common.serialization.SimpleStringSchema; // Decode byte data from Kafka to String
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment; // Flink execution environment
import org.apache.flink.streaming.api.datastream.DataStream;    // Flink stream type
import org.apache.flink.streaming.api.functions.KeyedProcessFunction; // Process function per key
import org.apache.flink.util.Collector;                         // Collector to emit new event

// =================== FLINK STATE ===================
import org.apache.flink.api.common.state.ValueState;           // State to store a single value per key
import org.apache.flink.api.common.state.ValueStateDescriptor; // Descriptor for ValueState

// =================== FLINK CONFIG ===================
import org.apache.flink.configuration.Configuration;           // Extended configuration (used in open method)

// =================== KAFKA ===================
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer; 

// =================== JSON ===================
import com.fasterxml.jackson.databind.ObjectMapper;            // JSON <-> Java object mapping
import com.fasterxml.jackson.databind.SerializationFeature;    // Jackson serialization config
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;   // Support for LocalDateTime

// =================== JAVA CORE ===================
import java.util.Properties;                                   // Kafka configuration
import java.time.Duration;                                     // Time difference calculation


public class App {

    // =================== CASE 1: Limit GPS events to HCM city ===================
    public static DataStream<gpsDTO> limit_area(DataStream<gpsDTO> df) {
        return df.filter(dto -> dto.getLatitude() >= 10.7 && dto.getLatitude() <= 10.9 
            && dto.getLongitude() >= 106.6 && dto.getLongitude() <= 106.8); 
    }

    // =================== CASE 2-4: Process abnormal GPS events ===================
    // Case 2: Detect abnormal speed (> 100 km/h)
    // Case 3: Handle reply events from Kafka
    // Case 4: Prevent spam based on status (available, no_available, on_trip)
    public static double calculate_Haversine(double lat1, double lng1, double lat2, double lng2) {
        final double R = 6371; // Earth radius in km

        double deltaLat = Math.toRadians(lat2-lat1); 
        double deltaLng = Math.toRadians(lng2-lng1); 

        // Haversine formula to calculate distance between two points on Earth
        double a = Math.sin(deltaLat / 2) * Math.sin(deltaLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2)) * Math.sin(deltaLng / 2) * Math.sin(deltaLng / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return R * c;
    }

    public static DataStream<gpsDTO> process_abnormal_gps(DataStream<gpsDTO> df){
        // Key by driver ID
        return df.keyBy(dto -> dto.getId()).process(new KeyedProcessFunction<String, gpsDTO, gpsDTO>() {
            // Thresholds
            private int speed_threshold = 100;          // Max speed in km/h

            // State to store previous GPS
            private ValueState<gpsDTO> prev_gpsDTO;

            @Override
            public void open(Configuration parameters) {
                // Previous GPS per driver
                ValueStateDescriptor<gpsDTO> prevDesc = new ValueStateDescriptor<>("previous-GPS", gpsDTO.class);
                prev_gpsDTO = getRuntimeContext().getState(prevDesc);
            }

            @Override
            public void processElement(gpsDTO current, Context ctx, Collector<gpsDTO> out) throws Exception {
                // Get previous GPS
                gpsDTO previous = prev_gpsDTO.value();

                // Null-safe check previous
                if (previous == null) {
                    prev_gpsDTO.update(current);
                    out.collect(current);
                    return;
                }

                // Calculate time difference, distance, and speed
                long deltaSeconds = Duration.between(previous.getTimestamp(), current.getTimestamp()).getSeconds();
                double distance = calculate_Haversine(previous.getLatitude(), previous.getLongitude(), current.getLatitude(), current.getLongitude());
                double speed = distance * 3600 / Math.max(deltaSeconds, 1); // If delta = 0, assign 1 to variable 'speed'. 1 will make an invalid event
                boolean isInvalid = deltaSeconds <= 0 || speed >= speed_threshold;

                // Handle invalid GPS
                if(isInvalid){
                    out.collect(previous);
                    return;
                }

                // Prevent spamming small movements
                if(speed < speed_threshold){
                    if(current.getStatus().equals("on_trip") && deltaSeconds <= 1) return;
                    if(current.getStatus().equals("available") && deltaSeconds <= 5) return;
                    if(current.getStatus().equals("no_available") && deltaSeconds <= 30) return;
                }
                
                // Emit valid event
                out.collect(current);
                prev_gpsDTO.update(current);
            }
        });
    }

    // =================== KAFKA CONFIG ===================
    public static Properties config_kafka(){
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "broker:29092"); // Kafka broker address
        prop.setProperty("group.id", "flink-gps-group");       // Consumer group
        return prop;
    }

    public static FlinkKafkaConsumer connect_consumer(String topic_name, Properties prop){
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                topic_name,
                new SimpleStringSchema(),
                prop
        );
        
        return consumer;
    }

    // =================== JSON TRANSFORMATION ===================
    public static DataStream<gpsDTO> transfer_JSON_to_JavaObj(DataStream<String> df_raw) {
        // Convert JSON string to gpsDTO object
        DataStream<gpsDTO> df = df_raw.map(new MapFunction<String, gpsDTO>() {
            @Override
            public gpsDTO map(String value) {
                try {
                    ObjectMapper mapper = new ObjectMapper();     // Jackson Datatype
                    mapper.registerModule(new JavaTimeModule()); // support LocalDateTime
                    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // disable timestamp format (it will not transfer TS to INT)

                    return mapper.readValue(value, gpsDTO.class);
                } catch (Exception e) {
                    System.err.println("Cannot parse JSON: " + value);
                    return null;
                }
            }
        });

        return df;
    }

    // =================== FULL TRANSFORMATION PIPELINE ===================
    public static DataStream<gpsDTO> transform_data(DataStream<String> df_raw){
        DataStream<gpsDTO> df1 = transfer_JSON_to_JavaObj(df_raw);      // JSON -> Object
        DataStream<gpsDTO> df2 = df1.filter(dto -> dto != null);        // Drop null
        DataStream<gpsDTO> df3 = limit_area(df2);                       // Limit area to HCM
        DataStream<gpsDTO> df4 = process_abnormal_gps(df3);             // Process abnormal GPS
        return df4;
    }

    // =================== MAIN ===================
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(); // Create Flink environment
        Properties prop = config_kafka();                                                     // Kafka properties
        FlinkKafkaConsumer<String> consumer = connect_consumer("gps-topic", prop);           // Kafka consumer
        DataStream<String> df_raw = env.addSource(consumer);                                  // Add Kafka source
        DataStream<gpsDTO> df = transform_data(df_raw);                                       // Apply transformations
        df.print();                                                                           // Print output
        env.execute("GPS pipeline");                                                          // Execute Flink job
    }
}