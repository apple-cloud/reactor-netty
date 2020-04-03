/*
 * Copyright (c) 2011-Present VMware, Inc. or its affiliates, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactor.netty.udp;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import io.netty.channel.ChannelOption;
import io.netty.util.NetUtil;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.resources.ConnectionProvider;
import reactor.netty.transport.AddressUtils;
import reactor.netty.transport.Transport;
import reactor.util.Logger;
import reactor.util.Loggers;

import static reactor.netty.ReactorNetty.format;

/**
 * A UdpServer allows to build in a safe immutable way a UDP server that is materialized
 * and connecting when {@link #bind()} is ultimately called.
 * <p>
 * <p> Example:
 * <pre>
 * {@code
 * UdpServer.create()
 *          .doOnBind(startMetrics)
 *          .doOnBound(startedMetrics)
 *          .doOnUnbind(stopMetrics)
 *          .host("127.0.0.1")
 *          .port(1234)
 *          .bind()
 *          .block()
 * }
 *
 * @author Stephane Maldini
 * @author Violeta Georgieva
 */
public class UdpServer extends Transport<UdpServer, UdpServerConfig> {

	static final UdpServer INSTANCE = new UdpServer();

	/**
	 * Prepare a {@link UdpServer}
	 *
	 * @return a {@link UdpServer}
	 */
	public static UdpServer create() {
		return UdpServer.INSTANCE;
	}

	final UdpServerConfig config;

	UdpServer() {
		this.config = new UdpServerConfig(
				Collections.singletonMap(ChannelOption.AUTO_READ, false),
				() -> new InetSocketAddress(NetUtil.LOCALHOST, DEFAULT_PORT));
	}

	UdpServer(UdpServerConfig config) {
		this.config = config;
	}

	/**
	 * Binds the {@link UdpServer} and returns a {@link Mono} of {@link Connection}. If
	 * {@link Mono} is cancelled, the underlying binding will be aborted. Once the {@link
	 * Connection} has been emitted and is not necessary anymore, disposing the main server
	 * loop must be done by the user via {@link Connection#dispose()}.
	 *
	 * @return a {@link Mono} of {@link Connection}
	 */
	public Mono<Connection> bind() {
		UdpServer dup;
		ConnectionObserver lifecycleObserver = config.lifecycleObserver();
		if (lifecycleObserver != null) {
			dup = observe(lifecycleObserver);
		}
		else {
			dup = duplicate();
		}

		UdpServerConfig config = dup.configuration();
		Mono<Connection> mono = ConnectionProvider.newConnection()
		                                          .acquire(config, null, null);
		if (config.doOnBind() != null) {
			mono = mono.doOnSubscribe(s -> config.doOnBind().accept(config));
		}
		return mono;
	}

	/**
	 * Starts the server in a blocking fashion, and waits for it to finish initializing
	 * or the startup timeout expires (the startup timeout is {@code 45} seconds). The
	 * returned {@link Connection} offers simple server API, including to {@link
	 * Connection#disposeNow()} shut it down in a blocking fashion.
	 *
	 * @return a {@link Connection}
	 */
	public final Connection bindNow() {
		return bindNow(Duration.ofSeconds(45));
	}

	/**
	 * Start the server in a blocking fashion, and wait for it to finish initializing
	 * or the provided startup timeout expires. The returned {@link Connection}
	 * offers simple server API, including to {@link Connection#disposeNow()}
	 * shut it down in a blocking fashion.
	 *
	 * @param timeout max startup timeout
	 * @return a {@link Connection}
	 */
	public final Connection bindNow(Duration timeout) {
		Objects.requireNonNull(timeout, "timeout");
		try {
			return Objects.requireNonNull(bind().block(timeout), "aborted");
		}
		catch (IllegalStateException e) {
			if (e.getMessage().contains("blocking read")) {
				throw new IllegalStateException("UdpServer couldn't be started within " + timeout.toMillis() + "ms");
			}
			throw e;
		}
	}

	@Override
	public UdpServerConfig configuration() {
		return config;
	}

	/**
	 * Set or add a callback called when {@link UdpServer} is about to start listening for incoming traffic.
	 *
	 * @param doOnBind a consumer observing connected events
	 * @return a new {@link UdpServer} reference
	 */
	public final UdpServer doOnBind(Consumer<? super UdpServerConfig> doOnBind) {
		Objects.requireNonNull(doOnBind, "doOnBind");
		UdpServer dup = duplicate();
		@SuppressWarnings("unchecked")
		Consumer<UdpServerConfig> current = (Consumer<UdpServerConfig>) dup.configuration().doOnBind;
		dup.configuration().doOnBind = current == null ? doOnBind : current.andThen(doOnBind);
		return dup;
	}

	/**
	 * Set or add a callback called after {@link UdpServer} has been started.
	 *
	 * @param doOnBound a consumer observing connected events
	 * @return a new {@link UdpServer} reference
	 */
	public final UdpServer doOnBound(Consumer<? super Connection> doOnBound) {
		Objects.requireNonNull(doOnBound, "doOnBound");
		UdpServer dup = duplicate();
		@SuppressWarnings("unchecked")
		Consumer<Connection> current = (Consumer<Connection>) dup.configuration().doOnBound;
		dup.configuration().doOnBound = current == null ? doOnBound : current.andThen(doOnBound);
		return dup;
	}

	/**
	 * Set or add a callback called after {@link UdpServer} has been shutdown.
	 *
	 * @param doOnUnbound a consumer observing unbound events
	 * @return a new {@link UdpServer} reference
	 */
	public final UdpServer doOnUnbound(Consumer<? super Connection> doOnUnbound) {
		Objects.requireNonNull(doOnUnbound, "doOnUnbound");
		UdpServer dup = duplicate();
		@SuppressWarnings("unchecked")
		Consumer<Connection> current = (Consumer<Connection>) dup.configuration().doOnUnbound;
		dup.configuration().doOnUnbound = current == null ? doOnUnbound : current.andThen(doOnUnbound);
		return dup;
	}

	/**
	 * Attach an IO handler to react on connected client
	 *
	 * @param handler an IO handler that can dispose underlying connection when {@link
	 * Publisher} terminates.
	 *
	 * @return a new {@link UdpServer}
	 */
	public final UdpServer handle(BiFunction<? super UdpInbound, ? super UdpOutbound, ? extends Publisher<Void>> handler) {
		Objects.requireNonNull(handler, "handler");
		return doOnBound(new OnBoundHandle(handler));
	}

	/**
	 * The host to which this server should bind.
	 *
	 * @param host the host to bind to.
	 * @return a new {@link UdpServer} reference
	 */
	public final UdpServer host(String host) {
		return localAddress(() -> AddressUtils.updateHost(configuration().localAddress(), host));
	}

	/**
	 * The port to which this server should bind.
	 *
	 * @param port The port to bind to.
	 * @return a new {@link UdpServer} reference
	 */
	public final UdpServer port(int port) {
		return localAddress(() -> AddressUtils.updatePort(configuration().localAddress(), port));
	}

	@Override
	protected UdpServer duplicate() {
		return new UdpServer(new UdpServerConfig(configuration()));
	}

	/**
	 * The default port for reactor-netty servers. Defaults to 12012 but can be tuned via
	 * the {@code PORT} <b>environment variable</b>.
	 */
	static final int DEFAULT_PORT = System.getenv("PORT") != null ? Integer.parseInt(System.getenv("PORT")) : 12012;

	static final Logger log = Loggers.getLogger(UdpServer.class);

	static final class OnBoundHandle implements Consumer<Connection> {

		final BiFunction<? super UdpInbound, ? super UdpOutbound, ? extends Publisher<Void>> handler;

		OnBoundHandle(BiFunction<? super UdpInbound, ? super UdpOutbound, ? extends Publisher<Void>> handler) {
			this.handler = handler;
		}

		@Override
		public void accept(Connection c) {
			if (log.isDebugEnabled()) {
				log.debug(format(c.channel(), "Handler is being applied: {}"), handler);
			}

			Mono.fromDirect(handler.apply((UdpInbound) c, (UdpOutbound) c))
			    .subscribe(c.disposeSubscriber());
		}
	}
}
