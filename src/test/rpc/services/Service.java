package test.rpc.services;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Date: 27.04.2018
 * Time: 12:46
 */
public class Service
{
  final Object service;

  public Service(Class service) throws Exception
  {
    if (service == null)
      throw new Exception("Service class is null");

    final Method[] methods = service.getMethods();
    if (methods == null || methods.length <= 0)
      throw new Exception("There are no accessible methods in service class: " + service.getName());

    this.service = service.newInstance();
  }

  public Object call(String method, Object[] params)
      throws NoSuchMethodException, InvocationTargetException, IllegalAccessException
  {
    Class[] args = null;
    if (params != null) {
      args = new Class[params.length];
      for (int i=0; i<params.length; i++) args[i] = params[i].getClass();
    }
    final Method svcMethod = service.getClass().getMethod(method, args);
    final Object result = svcMethod.invoke(service, params);
    return (svcMethod.getReturnType().equals(Void.TYPE)) ? Void.TYPE : result;
  }
}
