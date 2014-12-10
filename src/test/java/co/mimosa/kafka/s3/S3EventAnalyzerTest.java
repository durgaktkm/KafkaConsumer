package co.mimosa.kafka.s3;

import co.mimosa.kafka.producer.MimosaProducer;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.regex.Pattern;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class S3EventAnalyzerTest {
  S3EventAnalyzer eventAnalyzer;
  AmazonS3Client amazonS3;
  String s3Bucket;
  String s3Id;
  String newLineReplacement;
  String dirSeparator;
  ObjectMapper objectMapper;
  MimosaProducer producer;

  @Before
  public void setUp() throws Exception {
    amazonS3 = mock(AmazonS3Client.class);
    s3Bucket = "bucket";
    s3Id ="id";
    newLineReplacement ="#";
    dirSeparator = "/";
    objectMapper = mock(ObjectMapper.class);
    eventAnalyzer = new S3EventAnalyzer(amazonS3,s3Bucket,s3Id,newLineReplacement,dirSeparator,objectMapper,null);

  }

  @Test
  public void testUploadData() throws Exception {

  }

  @Test
  public void testAnalyze() throws Exception {

  }

  @Test
  public void testUploadDataThrowsExceptionWhileParsingData() throws IOException {
    String keyName="key";
    S3File s3File = new S3File();
    when(objectMapper.writeValueAsString(s3File)).thenThrow(new IOException("Can't Parse the data"));
    eventAnalyzer.uploadData(keyName,s3File);
  }

  @Test(expected = NullPointerException.class)
  public void testUploadDataWithNullPointerException() throws Exception {
    String keyName="key";
    S3File s3File = new S3File();
    eventAnalyzer.uploadData(keyName,s3File);
  }

  @Test
  public void testUploadDataWithNoException() throws IOException {
    String keyName="key";
    S3File s3File = new S3File();
    when(objectMapper.writeValueAsString(s3File)).thenReturn("abc");
    verify(amazonS3).putObject((PutObjectRequest) any());
    eventAnalyzer.uploadData(keyName,s3File);
  }



  @Test
  public void testGetMatchingStringForSerialNumber() throws Exception {
    URL url = Resources.getResource("prepare.json");
    String messageStr = Resources.toString(url, Charsets.UTF_8);
    final String SERIALNUMBER_REGEX = "(?<=\"serialNumber\":\")\\d{10}";
    final Pattern SERIALNUMBER_PATTERN = Pattern.compile(SERIALNUMBER_REGEX);
    String matchingString = eventAnalyzer.getMatchingString(SERIALNUMBER_PATTERN, messageStr);
    assertThat(matchingString).isNotNull().isNotEmpty().isEqualTo("0000002859");

  }

  @Test
  public void testGetMatchingStringForEventType() throws Exception {
    URL url = Resources.getResource("prepare.json");
    String messageStr = Resources.toString(url, Charsets.UTF_8);
    final Pattern EVENT_TYPE_PATTERN = Pattern.compile("(?<=\"eventType\":\")\\w+");
    String matchingString = eventAnalyzer.getMatchingString(EVENT_TYPE_PATTERN, messageStr);
    assertThat(matchingString).isNotNull().isNotEmpty().isEqualTo("Prepare");

  }
}