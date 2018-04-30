package test.rpc;

import java.io.*;

/**
 * Date: 29.04.2018
 * Time: 8:50
 */
public class CallStream
{
  public final static int MIN_PACKET_SIZE = 1;
  public final static int MAX_PACKET_SIZE = 10000*1024;
  public final static String ERR_READ_PACKET = "Failed to read packet ";

  public static void writePacket(DataOutputStream os, Object obj) throws IOException
  {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
         ObjectOutputStream oos = new ObjectOutputStream(baos))
    {
      oos.writeObject(obj); oos.flush();
      os.writeInt(baos.size()); baos.writeTo(os); os.flush();
    }
  }

  public static byte[] makeResponse(Call.Response response) throws IOException
  {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
         ObjectOutputStream oos = new ObjectOutputStream(baos))
    {
      oos.writeObject(response); oos.flush();
      return baos.toByteArray();
    }
  }

  public static Call.Response readResponse(DataInputStream is, int seqNum)
      throws IOException, ClassNotFoundException
  {
    return makeResponse(readObject(is, seqNum), seqNum);
  }

  public static Call.Request readRequest(DataInputStream is, int seqNum)
      throws IOException, ClassNotFoundException
  {
    return makeRequest(readObject(is, seqNum), seqNum);
  }

  public static Call makeCall(byte[] buf, int seqNum)
      throws IOException, ClassNotFoundException
  {
    return new Call(seqNum, makeRequest(CallStream.readObject(buf, seqNum), seqNum));
  }

  private static Object readObject(DataInputStream is, int seqNum)
      throws IOException, ClassNotFoundException
  {
    return readObject(readPacket(is), seqNum);
  }

  private static Call.Response makeResponse(Object object, int seqNum) throws IOException
  {
    if (object instanceof Call.Response) return (Call.Response)object;
    else throw new IOException("Invalid response received for seqNum=" + seqNum + ", class=" + object.getClass());
  }

  private static Call.Request makeRequest(Object object, int seqNum) throws IOException
  {
    if (object instanceof Call.Request) return (Call.Request)object;
    else throw new IOException("Invalid request received for seqNum=" + seqNum + ", class=" + object.getClass());
  }

  private static Object readObject(byte[] buf, int seqNum)
      throws IOException, ClassNotFoundException
  {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(buf);
         ObjectInputStream ois = new ObjectInputStream(bais))
    {
      Object object = ois.readObject();
      if (object == null)
        throw new IOException("Null object received for seqNum=" + seqNum);
      else return object;
    }
  }

  private static byte[] readPacket(DataInputStream is) throws IOException
  {
    int size = is.readInt();
    if (size < CallStream.MIN_PACKET_SIZE || size > CallStream.MAX_PACKET_SIZE)
      throw new IOException(CallStream.ERR_READ_PACKET + "size=" + size + " is invalid. Max allowed packet size=" +
                            CallStream.MAX_PACKET_SIZE);
    byte buf[] = new byte[size];
    is.readFully(buf);
    return buf;
  }

}
