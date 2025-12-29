import java.time.LocalDateTime;

public class gpsDTO {
    public String id;
    public double latitude;
    public double longitude;
    public String status;
    public LocalDateTime timestamp;

    public gpsDTO() {}

    public gpsDTO(String id, double latitude, double longitude, String status, LocalDateTime timestamp) {
        this.id = id;
        this.latitude = latitude;
        this.longitude = longitude;
        this.status = status;
        this.timestamp = timestamp;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public LocalDateTime getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(LocalDateTime timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "gpsDTO{" +
            "id='" + id + '\'' +
            ", latitude=" + latitude +
            ", longitude=" + longitude +
            ", status='" + status + '\'' +
            ", timestamp=" + timestamp +
            '}';
    }
}