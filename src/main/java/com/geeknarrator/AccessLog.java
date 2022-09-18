package com.geeknarrator;

import java.util.Date;

public class AccessLog {
  private String ip;
  private Date dateTime;
  private String apiCall;
  private String httpCode;
  private String responseSize;
  private String httpUrl;
  private String userAgent;

  public AccessLog(String ip, Date dateTime, String apiCall, String httpCode, String responseSize, String httpUrl, String userAgent) {
    this.ip = ip;
    this.dateTime = dateTime;
    this.apiCall = apiCall;
    this.httpCode = httpCode;
    this.responseSize = responseSize;
    this.httpUrl = httpUrl;
    this.userAgent = userAgent;
  }

  public String getIp() {
    return ip;
  }

  public void setIp(String ip) {
    this.ip = ip;
  }

  public Date getDateTime() {
    return dateTime;
  }

  public void setDateTime(Date dateTime) {
    this.dateTime = dateTime;
  }

  public String getApiCall() {
    return apiCall;
  }

  public void setApiCall(String apiCall) {
    this.apiCall = apiCall;
  }

  public String getUserAgent() {
    return userAgent;
  }

  public void setUserAgent(String userAgent) {
    this.userAgent = userAgent;
  }

  public String getHttpCode() {
    return httpCode;
  }

  public void setHttpCode(String httpCode) {
    this.httpCode = httpCode;
  }

  public String getResponseSize() {
    return responseSize;
  }

  public void setResponseSize(String responseSize) {
    this.responseSize = responseSize;
  }

  public String getHttpUrl() {
    return httpUrl;
  }

  public void setHttpUrl(String httpUrl) {
    this.httpUrl = httpUrl;
  }

  @Override
  public String toString() {
    return "AccessLog{" +
        "ip='" + ip + '\'' +
        ", dateTime='" + dateTime + '\'' +
        ", apiCall='" + apiCall + '\'' +
        ", httpCode='" + httpCode + '\'' +
        ", responseSize='" + responseSize + '\'' +
        ", httpUrl='" + httpUrl + '\'' +
        ", userAgent='" + userAgent + '\'' +
        '}';
  }
}
