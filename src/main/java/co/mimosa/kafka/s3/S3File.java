package co.mimosa.kafka.s3;

import org.codehaus.jackson.annotate.JsonRawValue;

/**
 * Created by ramdurga on 11/24/14.
 */
public class S3File {
  private long time_stamp;
  private String serialNumber;
  @JsonRawValue
  private String raw_json;

  public long getTime_stamp() {
    return time_stamp;
  }

  @Override
  public String toString() {
    return "S3File[time_stamp:" + time_stamp + ", serialNumber:" + serialNumber + ", raw_json:" + raw_json + "]";
  }

  public void setTime_stamp(long time_stamp) {
    this.time_stamp = time_stamp;
  }


  public String getRaw_json() {
    return raw_json;
  }

  public void setRaw_json(String raw_json) {
    this.raw_json = raw_json;
  }

  public String getSerialNumber() {
    return serialNumber;
  }

  public void setSerialNumber(String serialNumber) {
    this.serialNumber = serialNumber;
  }
}
