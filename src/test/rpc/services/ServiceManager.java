package test.rpc.services;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import test.rpc.Call;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

/**
 * Class implements RPC to registered Services.
 *
 * On start loads Services and registers it's instances by names (as defined in serviceProperties map).
 * Provides method call() to call Services methods. Return call result, including possible Exceptions.
 *
 * Date: 27.04.2018
 * Time: 12:46
 */
public class ServiceManager
{
  private final Logger logger = LogManager.getLogger(this.getClass());
  private final HashMap<String, Service> services = new HashMap<>();

  public ServiceManager(Properties serviceProperties) throws Exception
  {
    if (serviceProperties == null)
      throw new Exception("Services config is null");

    for (String service : serviceProperties.stringPropertyNames()) {
      final String serviceClassName = serviceProperties.getProperty(service);
      try {
        this.services.put(service, new Service(getClass().getClassLoader().loadClass(serviceClassName)));
        logger.info("Loaded service=" + service + ", class=" + serviceClassName);
      } catch (Exception e) {
        throw new Exception("Failed to load class=" + serviceClassName + " for service=" + service);
      }
    }
  }

  public void call(Call call)
  {
    final Call.Request request = call.getRequest();

    final String service = request.getService();
    final String method = request.getMethod();
    final Object[] params = request.getParams();

    logger.info("Call service=" + service + ", method=" + method + " with params: " + Arrays.toString(params));
    try {
      if (service == null)
        throw new Exception("Service to call is null");
      final Service svc = services.get(service);
      if (svc == null)
        throw new Exception("Service=" + service + " is unknown");

      final Object result = svc.call(method, params);
      call.setResponse(new Call.Response(result));
    }
    catch (Exception exc) {
      call.setResponse(new Call.Response(exc));
    }
    final String result = (call.getResponse().getResult() != null) ?
                           call.getResponse().getResult().toString() :
                           call.getResponse().getExc().toString();
    logger.info("Result service=" + service + ", method=" + method + " : " + result);
  }
}
