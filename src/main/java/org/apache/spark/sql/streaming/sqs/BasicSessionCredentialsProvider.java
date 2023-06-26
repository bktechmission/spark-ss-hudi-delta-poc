package org.apache.spark.sql.streaming.sqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicSessionCredentials;
import org.apache.commons.lang.StringUtils;

public class BasicSessionCredentialsProvider implements AWSCredentialsProvider {
  private final String accessKey;
  private final String secretKey;
  private final String sessionToken;

  public BasicSessionCredentialsProvider(String accessKey, String secretKey, String sessionToken) {
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.sessionToken = sessionToken;
  }

  public AWSCredentials getCredentials() {
    if (!StringUtils.isEmpty(accessKey)
            && !StringUtils.isEmpty(secretKey) && !StringUtils.isEmpty(sessionToken)) {
      return new BasicSessionCredentials(accessKey, secretKey, sessionToken);
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
