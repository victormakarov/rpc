package test.rpc.services.sample;

import java.util.Date;

/**
 * Date: 27.04.2018
 * Time: 14:54
 */
public class Service1
{
  public void sleep(Long millis) {
    try { Thread.sleep(millis.longValue()); }
    catch (InterruptedException e) { e.printStackTrace(); }
  }
  public Date getCurrentDate() {
    return new Date();
  }

  public void mPubVoid() {
    System.out.println("mPubVoid");
  }
  public Integer mPubInteger() {
    System.out.println("mPubInteger");
    return 1;
  }
  public Integer mPubInteger4Integer(Integer val) {
    System.out.println("mPubInteger4Integer");
    return val;
  }
  public void mPubVoidExc() throws ExceptionInInitializerError {
    System.out.println("mPubVoidExc");
    throw new ExceptionInInitializerError("mPubVoidExc Exc");
  }

  private void mPvtVoid() {
    System.out.println("mPvtVoid");
  }
}
