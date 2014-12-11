package co.mimosa.kafka.s3;

import co.mimosa.kafka.callable.IEventAnalyzer;
import co.mimosa.kafka.producer.MimosaProducer;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ramdurga on 11/24/14.
 */
public class S3EventAnalyzer implements IEventAnalyzer {
  private static final Logger logger = LoggerFactory.getLogger(S3EventAnalyzer.class);
  private static final String SERIALNUMBER_REGEX = "(?<=\"serialNumber\":\")\\d{10}";
  private static final Pattern SERIALNUMBER_PATTERN = Pattern.compile(SERIALNUMBER_REGEX);
  private static final Pattern EVENT_TYPE_PATTERN = Pattern.compile("(?<=\"eventType\":\")\\w+");


  private final AmazonS3Client s3Client;
  private final String s3Bucket;
  private final String s3Id;
  private final String newLineReplacement;
  private final String dirSeparator;
  private final ObjectMapper objectMapper;
  private final MimosaProducer producer;
  private String errorTopic;

  public S3EventAnalyzer(AmazonS3Client amazonS3Client, String s3Bucket, String s3Id,
       String newLineReplacement, String dirSeparator,ObjectMapper objectMapper,MimosaProducer producer,String errorTopic) {

    s3Client = amazonS3Client;
    this.s3Bucket = s3Bucket;
    this.s3Id = s3Id;
    this.newLineReplacement = newLineReplacement;
    this.dirSeparator = dirSeparator;
    this.objectMapper = objectMapper;
    this.producer = producer;
    this.errorTopic = errorTopic;
  }



  void uploadData(String keyName, S3File deviceResponseDetail) {
    System.out.printf("received data for s3");

    try {
      String data="" ;
      try {
        data = objectMapper.writeValueAsString(deviceResponseDetail);
      } catch (Exception e) {
        logger.error("Error while creating JSON string for S3");
        PutObjectResult result = putS3File(keyName, data);
        return;
      }
      System.out.print("....uploading to s3");
      logger.debug("Uploading a new object to S3 from a file\n");
      PutObjectResult result = putS3File(keyName, data);
      logger.debug("S3 Response + " + result);
      System.out.println("....done");
    } catch (AmazonServiceException ase) {
      logger.warn("Error while uploading device action response on S3");
      logger.debug("Caught an AmazonServiceException, which " + "means your request made it "
          + "to Amazon S3, but was rejected with an error response" + " for some reason.");
      logger.debug("Error Message:    " + ase.getMessage());
      logger.debug("HTTP Status Code: " + ase.getStatusCode());
      logger.debug("AWS Error Code:   " + ase.getErrorCode());
      logger.debug("Error Type:       " + ase.getErrorType());
      logger.debug("Request ID:       " + ase.getRequestId());
      throw ase;
    } catch (AmazonClientException ace) {
      logger.warn("Error while uploading device action response on S3");
      logger.debug("Caught an AmazonClientException, which " + "means the client encountered "
          + "an internal error while trying to " + "communicate with S3, "
          + "such as not being able to access the network.");
      logger.debug("Error Message: " + ace.getMessage());
      throw ace;
    }
  }

  private PutObjectResult putS3File(String keyName,  String data) {
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(data.length());
    return s3Client.putObject(new PutObjectRequest(s3Bucket, keyName, new ByteArrayInputStream(data.getBytes()),
        objectMetadata));
  }

  String getMatchingString(Pattern pattern, String jsonString) {
    Matcher matcher = pattern.matcher(jsonString);
    if(matcher.find())
      return matcher.group();
    return null;
  }

  @Override public Boolean analyze(String eventJsonString) {
    String eventType;
    String serialNumber;
    eventJsonString = eventJsonString.replaceAll("\n", newLineReplacement);
    int eventDataEllipsisSize = (eventJsonString.length() > 65 ? 65 : eventJsonString.length());
    logger.debug("Hash replaced content={}", eventJsonString.substring(0, eventDataEllipsisSize).trim() + "...");
    Calendar cal  = new GregorianCalendar();
    cal.setTime(new Date());

    try{
      serialNumber = getMatchingString(SERIALNUMBER_PATTERN, eventJsonString);
      eventType = getMatchingString(EVENT_TYPE_PATTERN, eventJsonString);
    }catch(Exception e){
      String key = s3Id+dirSeparator+cal.get(Calendar.YEAR)+dirSeparator+(cal.get(Calendar.MONTH)+1)+dirSeparator+cal.get(Calendar.DAY_OF_MONTH)+dirSeparator+"unknown"+dirSeparator+"unknown_"+cal.getTimeInMillis()+".json";
      putS3File(key, eventJsonString);
      return true;
    }

    try{

      S3File file = new S3File();
      file.setRaw_json(eventJsonString);
      file.setSerialNumber(serialNumber);
      file.setTime_stamp(cal.getTimeInMillis());
//      //TODO remove after talking to venkatesh. This may be performance issue .
//      if(!s3Client.doesBucketExist(s3Bucket)) {
//        s3Client.createBucket(s3Bucket);
//      }
      String key = s3Id+dirSeparator+cal.get(Calendar.YEAR)+dirSeparator+(cal.get(Calendar.MONTH)+1)+dirSeparator+cal.get(Calendar.DAY_OF_MONTH)+dirSeparator+serialNumber+dirSeparator+eventType+dirSeparator+eventType+"_"+cal.getTimeInMillis()+".json";
      uploadData(key,file);
    }catch(Exception e){
      logger.error("Error while uploading JSON string to S3"+e.getMessage());
       //Resolution is to put in a separate queue here.
      producer.sendDataToKafka(errorTopic,eventJsonString);
    }
    return true;
  }

}
