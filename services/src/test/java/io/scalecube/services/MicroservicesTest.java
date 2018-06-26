package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.services.Microservices.Builder;
import io.scalecube.services.Microservices.DispatcherContext;
import io.scalecube.services.Microservices.ProxyContext;
import io.scalecube.services.routing.RoundRobinServiceRouter;
import io.scalecube.testlib.BaseTest;

import io.scalecube.transport.Message;
import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class MicroservicesTest extends BaseTest {

  @Test
  public void test_microservices_config() {
    Builder builder = new Builder();
    ServicesConfig servicesConfig = ServicesConfig.builder(builder).create();
    Microservices micro = Microservices.builder().services(servicesConfig).build();
    assertTrue(servicesConfig.services().isEmpty());
    assertTrue(micro.services().isEmpty());
    micro.shutdown();
  }

  @Test
  public void test_microservices_dispatcher_router() {
    Builder builder = new Builder();
    ServicesConfig servicesConfig = ServicesConfig.builder(builder).create();
    Microservices micro = Microservices.builder().services(servicesConfig).build();
    DispatcherContext dispatcher = micro.dispatcher();
    dispatcher.router(RoundRobinServiceRouter.class);
    assertTrue(dispatcher.router().equals(RoundRobinServiceRouter.class));
    assertTrue(servicesConfig.services().isEmpty());
    micro.shutdown();
  }

  @Test
  public void test_microservices_proxy_router() {
    Builder builder = new Builder();
    ServicesConfig servicesConfig = ServicesConfig.builder(builder).create();
    Microservices micro = Microservices.builder().services(servicesConfig).build();
    ProxyContext proxy = micro.proxy();

    assertTrue(proxy.router().equals(RoundRobinServiceRouter.class));
    micro.shutdown();
  }

  @Test
  public void test_microservices_unregister() {
    GreetingServiceImpl greeting = new GreetingServiceImpl();
    Microservices micro = Microservices.builder().services(greeting).build();
    assertEquals(micro.services().size(), 1);
    micro.unregisterService(greeting);
    assertEquals(micro.services().size(), 0);

    try {
      micro.unregisterService(null);
    } catch (Exception ex) {
      assertEquals("Service object can't be null.", ex.getMessage().toString());
    }
    micro.shutdown();
  }

  @Test
  public void test_microservices_different_timeout() throws InterruptedException, ExecutionException, TimeoutException {
    Duration duration2 = Duration.ofSeconds(1);
    Duration duration1 = Duration.ofSeconds(duration2.getSeconds()*3);
    GreetingService greeting = new GreetingService() {

      @Override
      public CompletableFuture<String> greetingNoParams() {
        throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<String> greeting(String string) {
        try {
          Thread.sleep(TimeUnit.SECONDS.toMillis(duration2.getSeconds()*2));
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return CompletableFuture.completedFuture("DONE");
      }

      @Override
      public CompletableFuture<GreetingResponse> greetingRequestTimeout(GreetingRequest request) {
        throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<GreetingResponse> greetingRequest(GreetingRequest string) {
        throw new UnsupportedOperationException();
      }

      @Override
      public CompletableFuture<Message> greetingMessage(Message request) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void greetingVoid(GreetingRequest request) {
        throw new UnsupportedOperationException();
      }
    };
    Microservices provider = Microservices.builder().services(greeting).build();
    Microservices consumer = Microservices.builder().seeds(provider.cluster().address()).build();
    ProxyContext proxy1 = consumer.proxy();
    ProxyContext proxy2 = consumer.proxy();
    assertNotEquals(proxy1, proxy2);
    GreetingService service1 = proxy1.timeout(duration1).api(GreetingService.class).create();
    /*GreetingService service2 = */proxy2.timeout(duration2).api(GreetingService.class).create();
    service1.greeting("hello").get(duration1.getSeconds(), TimeUnit.SECONDS);
  }
}
