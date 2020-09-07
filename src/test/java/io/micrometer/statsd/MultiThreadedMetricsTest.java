package io.micrometer.statsd;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.Counter;
import io.micrometer.shaded.io.netty.channel.ChannelHandlerContext;
import io.micrometer.shaded.io.netty.channel.ChannelOutboundHandlerAdapter;
import io.micrometer.shaded.io.netty.channel.ChannelPromise;
import io.micrometer.shaded.io.netty.handler.logging.LogLevel;
import io.micrometer.shaded.io.netty.handler.logging.LoggingHandler;
import io.micrometer.shaded.reactor.core.publisher.Flux;
import io.micrometer.shaded.reactor.netty.Connection;
import io.micrometer.shaded.reactor.netty.DisposableChannel;
import io.micrometer.shaded.reactor.netty.tcp.TcpServer;
import io.micrometer.shaded.reactor.netty.udp.UdpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import static org.awaitility.Awaitility.await;

class MultiThreadedMetricsTest {

	StatsdMeterRegistry meterRegistry;
	DisposableChannel server;
	CountDownLatch serverLatch;
	AtomicInteger serverMetricReadCount = new AtomicInteger();

	volatile boolean bound = false;

	@AfterEach
	void cleanUp() {
		meterRegistry.close();
		if (server != null) {
			server.disposeNow();
		}
	}

	private static class FastCountingThread extends Thread {

		final Counter counter;

		FastCountingThread(Counter counter) {
			this.counter = counter;
			setDaemon(true);
			setName(counter.getId().getName());
		}

		public void run() {
			while (!interrupted()) {
				counter.increment();
			}
			LoggerFactory.getLogger(FastCountingThread.class).warn("Stopped sending metrics");
		}
	}

	@Test
	void resumeSendingMetrics_whenServerIntermittentlyFails() throws InterruptedException {
		StatsdProtocol protocol = StatsdProtocol.UDP;
		serverLatch = new CountDownLatch(1);
		AtomicInteger writeCount = new AtomicInteger();
		server = startServer(protocol, 0);

		final int port = server.address().getPort();

		meterRegistry = new StatsdMeterRegistry(getBufferedConfig(protocol, port), Clock.SYSTEM);
		startRegistryAndWaitForClient();
		trackWritesForUdpClient(protocol, writeCount);
		new FastCountingThread(Counter.builder("my.counter.1").register(meterRegistry)).start();
		new FastCountingThread(Counter.builder("my.counter.2").register(meterRegistry)).start();
		new FastCountingThread(Counter.builder("my.counter.3").register(meterRegistry)).start();
		new FastCountingThread(Counter.builder("my.counter.4").register(meterRegistry)).start();
		new FastCountingThread(Counter.builder("my.counter.5").register(meterRegistry)).start();
		new FastCountingThread(Counter.builder("my.counter.6").register(meterRegistry)).start();
		new FastCountingThread(Counter.builder("my.counter.7").register(meterRegistry)).start();
		new FastCountingThread(Counter.builder("my.counter.8").register(meterRegistry)).start();
		new FastCountingThread(Counter.builder("my.counter.9").register(meterRegistry)).start();


		while (true) {
			server.disposeNow();
			server = startServer(protocol, port);
			Thread.sleep(7);
		}
	}

	private void trackWritesForUdpClient(StatsdProtocol protocol, AtomicInteger writeCount) {
		if (protocol == StatsdProtocol.UDP) {
			await().until(() -> meterRegistry.statsdConnection.get() != null);
			((Connection) meterRegistry.statsdConnection.get())
					.addHandler(new LoggingHandler("udpclient", LogLevel.INFO))
					.addHandler(new ChannelOutboundHandlerAdapter() {
						@Override
						public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
							writeCount.incrementAndGet();
							super.write(ctx, msg, promise);
						}
					});
		}
	}

	private void startRegistryAndWaitForClient() {
		meterRegistry.start();
		await().until(() -> !clientIsDisposed());
	}

	private boolean clientIsDisposed() {
		return meterRegistry.statsdConnection.get().isDisposed();
	}

	private DisposableChannel startServer(StatsdProtocol protocol, int port) {
		if (protocol == StatsdProtocol.UDP) {
			return UdpServer.create()
					.host("localhost")
					.port(port)
					.handle((in, out) ->
							in.receive().asString()
									.flatMap(packet -> {
										serverLatch.countDown();
										serverMetricReadCount.getAndIncrement();
										return Flux.never();
									}))
					.doOnBound((server) -> bound = true)
					.doOnUnbound((server) -> bound = false)
					.wiretap("udpserver", LogLevel.INFO)
					.bindNow(Duration.ofSeconds(120));
		}
		else if (protocol == StatsdProtocol.TCP) {
			AtomicReference<DisposableChannel> channel = new AtomicReference<>();
			return TcpServer.create()
					.host("localhost")
					.port(port)
					.handle((in, out) ->
							in.receive().asString()
									.flatMap(packet -> {
										IntStream.range(0, packet.split("my.counter").length - 1).forEach(i -> {
											serverLatch.countDown();
											serverMetricReadCount.getAndIncrement();
										});
										in.withConnection(channel::set);
										return Flux.never();
									}))
					.doOnBound((server) -> bound = true)
					.doOnUnbound((server) -> {
						bound = false;
						if (channel.get() != null) {
							channel.get().dispose();
						}
					})
					.wiretap("tcpserver", LogLevel.INFO)
					.bindNow(Duration.ofSeconds(5));
		}
		else {
			throw new IllegalArgumentException("test implementation does not currently support the protocol " + protocol);
		}
	}

	private StatsdConfig getBufferedConfig(StatsdProtocol protocol, int port) {
		return new StatsdConfig() {
			@Override
			public String get(String key) {
				return null;
			}

			@Override
			public int port() {
				return port;
			}

			@Override
			public StatsdProtocol protocol() {
				return protocol;
			}

			@Override
			public boolean buffered() {
				return true;
			}

			public Duration pollingFrequency() {
				return Duration.ofMillis(100);
			}

			public int maxPacketLength() {
				return 10_000;
			}
		};
	}
}