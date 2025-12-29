import org.apache.flink.api.common.functions.MapFunction; // Hàm map chuyển đổi từng phần tử
import org.apache.flink.api.common.serialization.SimpleStringSchema; // Giải mã dữ liệu thành chuỗi (Kafka trả data là byte, ông này giải mã sang string)
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment; // Khởi tạo môi trường flink
import org.apache.flink.streaming.api.datastream.DataStream; // Kiểu dữ liệu 
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer; // Connector Flink-Kafka

import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper; // JSON ↔ Java object
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;


public class App {

    public double calculate_Haversine(double lat1, double lng1, double lat2, double lng2) {
        final double R = 6731; // Độ trái đất

        double deltaLat = Math.toRadians(lat2-lat1); 
        double deltaLng = Math.toRadians(lng2-lng1); 

        // Haversine
        // a = sin(1/2 * delta(lat))^2 + cos(lat1) * cos(lat2) * sin(1/2 * deltaLng)^2
        double a = Math.sin(deltaLat/2) * Math.sin(deltaLat/2) + Math.cos(lat1) * Math.cos(lat2) * Math.sin(deltaLng/2) * Math.sin(deltaLng/2)
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return R * c;
    }

    public void process_abnormal_gps(DataStream<gpsDTO> df){
        df.keyBy(dto -> dto.getId()).process(new KeyedProcessFunction<String, gpsDTO, String>() {
            private ValueState<gpsDTO> prev_gpsDTO;

            // Khởi tạo state
            @Override
            public void open() {
                prev_gpsDTO = getRuntimeContext().getState(new ValueStateDescriptor<>("previous-GPS", gpsDTO.class));
            }

            @Override
            public void processElement(gpsDTO current_gpsDTO, Context ctx, Collector<String> out) throws Exception {
                gpsDTO last = prev_gpsDTO.value();
            }
        });
    }

    public static void main(String[] args) throws Exception {

        // 1. Tạo environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Cấu hình Kafka
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "broker:29092"); // Kafka broker
        prop.setProperty("group.id", "flink-gps-group"); // Consumer group

        // 3. Kafka consumer
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "gps-topic",
                new SimpleStringSchema(),
                prop
        );

        // 4. Thêm source
        DataStream<String> df_raw = env.addSource(consumer);

        // 5. Transformation
        // JSON -> gpsDTO
        DataStream<gpsDTO> df_mapped = df_raw.map(new MapFunction<String, gpsDTO>() {
            @Override
            public gpsDTO map(String value) {
                try {
                    ObjectMapper mapper = new ObjectMapper(); // Khai báo kiểu dữ liệu của Jackson để parse JSON sang Java Obj
                    mapper.registerModule(new com.fasterxml.jackson.datatype.jsr310.JavaTimeModule()); // Đăng ký module để hiểu các kiểu tgian
                    mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS); // Tắt chế độ chuyển ngày giờ sang số 

                    return mapper.readValue(value, gpsDTO.class);
                } catch (Exception e) {
                    System.err.println("Cannot parse JSON: " + value);
                    return null;
                }
            }
        });
        
        // Loại bỏ null
        DataStream<gpsDTO> df_final = df_mapped.filter(dto -> dto != null);

        // Lat & Lng thuộc vào Tp. HCM
        df_final = df_final.filter(dto -> 
            dto.getLatitude() >= 10.7 &&
            dto.getLatitude() <= 10.9 &&
            dto.getLongitude() >= 106.6 &&
            dto.getLongitude() <= 106.8
        );
        
        // 7. Thực thi job
        env.execute("GPS pipeline");

    }
}
