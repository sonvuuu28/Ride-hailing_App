import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

public class DriverGpsRedisSink extends RichSinkFunction<gpsDTO> {

    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) {
        jedis = new Jedis("redis", 6379);
        jedis.auth("123");
    }

    @Override
    public void invoke(gpsDTO value, Context context) {
        String key = "driver:gps";
        String field = value.getId();

        String json = String.format(
            "{\"lat\":%f,\"lng\":%f,\"status\":\"%s\"}",
            value.getLatitude(),
            value.getLongitude(),
            value.getStatus()
        );

        jedis.hset(key, field, json);
    }

    @Override
    public void close() {
        if (jedis != null) {
            jedis.close();
        }
    }
}
