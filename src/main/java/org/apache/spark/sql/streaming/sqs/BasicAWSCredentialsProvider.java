package org.apache.spark.sql.streaming.sqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.AWSCredentials;
import org.apache.commons.lang.StringUtils;

public class BasicAWSCredentialsProvider implements AWSCredentialsProvider {
  private final String accessKey;
  private final String secretKey;

  public BasicAWSCredentialsProvider(String accessKey, String secretKey) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  public AWSCredentials getCredentials() {
    if (!StringUtils.isEmpty(accessKey) && !StringUtils.isEmpty(secretKey)) {
      return new BasicAWSCredentials(accessKey, secretKey);
    }
    throw new AmazonClientException(
        "Access key or secret key is null");
  }

  public void refresh() {}

  @Override
  public String toString() {
    return getClass().getSimpleName();
  }

}
