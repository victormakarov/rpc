package test.rpc;

import java.io.Serializable;

/**
 * Date: 27.04.2018
 * Time: 19:10
 */
public class Call
{
  private final int seqNum;

  private Request request;
  private Response response;
  private Exception exc;

  public Call(int seqNum, Request request) {
    this.seqNum = seqNum; this.request = request; response = null; exc = null;
  }
  public Call(int seqNum, String service, String method, Object[] params) {
    this(seqNum, new Request(service, method, params));
  }

  public int getSeqNum() {
    return this.seqNum;
  }

  public void setResponse(Response response) {
    this.response = response;
  }
  public Request getRequest() {
    return this.request;
  }
  public Response getResponse() {
    return this.response;
  }

  public Exception getExc() {
    return exc;
  }
  public void setExc(Exception exc) {
    this.exc = exc;
  }

  public static class Request implements Serializable {
    private final String service;
    private final String method;
    private final Object[] params;

    private Request(String service, String method, Object[] params) {
      this.service = service; this.method = method; this.params = params;
    }

    public String getService() {
      return service;
    }
    public String getMethod() {
      return method;
    }
    public Object[] getParams() {
      return params;
    }
  }

  public static class Response implements Serializable {
    private final Exception exc;
    private final Object result;

    public Response(Exception exc) {
      this.exc = exc; this.result = null;
    }
    public Response(Object res) {
      this.result = res; this.exc = null;
    }

    public Exception getExc() {
      return exc;
    }
    public Object getResult() {
      return result;
    }
  }
}
