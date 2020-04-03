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

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFactory;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ReflectiveChannelFactory;
import io.netty.channel.socket.DatagramChannel;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.logging.LoggingHandler;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.NettyPipeline;
import reactor.netty.channel.ByteBufAllocatorMetrics;
import reactor.netty.channel.ChannelMetricsHandler;
import reactor.netty.channel.ChannelMetricsRecorder;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.channel.MicrometerChannelMetricsRecorder;
import reactor.netty.resources.LoopResources;
import reactor.netty.transport.TransportConfig;
import reactor.util.Logger;
import reactor.util.Loggers;

import javax.annotation.Nullable;
import java.net.SocketAddress;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static reactor.netty.ReactorNetty.format;

/**
 * Encapsulate all necessary configuration for UDP client transport.
 *
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public final class UdpServerConfig extends TransportConfig {

	Consumer<? super UdpServerConfig> doOnBind;
	Consumer<? super Connection>      doOnBound;
	Consumer<? super Connection>      doOnUnbound;

	UdpServerConfig(Map<ChannelOption<?>, ?> options, Supplier<? extends SocketAddress> localAddress) {
		super(options, localAddress);
	}

	UdpServerConfig(UdpServerConfig parent) {
		super(parent);
		this.doOnBind = parent.doOnBind;
		this.doOnBound = parent.doOnBound;
		this.doOnUnbound = parent.doOnUnbound;
	}

	/**
	 * Return the configured callback
	 *
	 * @return the configured callback
	 */
	@Nullable
	public final Consumer<? super UdpServerConfig> doOnBind() {
		return doOnBind;
	}

	/**
	 * Return the configured callback
	 *
	 * @return the configured callback
	 */
	@Nullable
	public final Consumer<? super Connection> doOnBound() {
		return doOnBound;
	}

	/**
	 * Return the configured callback
	 *
	 * @return the configured callback
	 */
	@Nullable
	public final Consumer<? super Connection> doOnUnbound() {
		return doOnUnbound;
	}

	@Override
	protected ChannelInitializer<Channel> channelInitializer(ChannelOperations.OnSetup channelOperationsProvider,
			ConnectionObserver connectionObserver, @Nullable SocketAddress remoteAddress) {
		return new UdpServerChannelInitializer(this, channelOperationsProvider, connectionObserver, remoteAddress);
	}

	@Override
	protected ChannelOperations.OnSetup channelOperationsProvider() {
		return DEFAULT_OPS;
	}

	@Override
	protected ChannelFactory<? extends Channel> connectionFactory(EventLoopGroup elg) {
		LoopResources loopResources = loopResources() != null ? loopResources() : defaultLoopResources();
		ChannelFactory<DatagramChannel> channelFactory;
		if (isPreferNative()) {
			channelFactory = new ReflectiveChannelFactory<>(loopResources.onDatagramChannel(elg));
		}
		else {
			channelFactory = () -> new NioDatagramChannel(family());
		}
		return channelFactory;
	}

	@Override
	protected LoggingHandler defaultLoggingHandler() {
		return LOGGING_HANDLER;
	}

	@Override
	protected LoopResources defaultLoopResources() {
		return UdpResources.get();
	}

	@Override
	protected ChannelMetricsRecorder defaultMetricsRecorder() {
		return MicrometerUdpServerMetricsRecorder.INSTANCE;
	}

	@Override
	protected EventLoopGroup eventLoopGroup() {
		LoopResources loopResources = loopResources() != null ? loopResources() : defaultLoopResources();
		return loopResources.onClient(isPreferNative());
	}

	@Nullable
	@Override
	protected ConnectionObserver lifecycleObserver() {
		if (doOnBound() == null && doOnUnbound() == null) {
			return null;
		}
		return new UdpServerDoOn(doOnBound(), doOnUnbound());
	}

	static final ChannelOperations.OnSetup DEFAULT_OPS = (ch, c, msg) -> new UdpOperations(ch, c);

	static final Logger log = Loggers.getLogger(UdpServerConfig.class);

	static final LoggingHandler LOGGING_HANDLER = new LoggingHandler(UdpServer.class);

	static final class MicrometerUdpServerMetricsRecorder extends MicrometerChannelMetricsRecorder {

		static final MicrometerUdpServerMetricsRecorder INSTANCE =
				new MicrometerUdpServerMetricsRecorder(reactor.netty.Metrics.UDP_SERVER_PREFIX, "udp");

		MicrometerUdpServerMetricsRecorder(String name, String protocol) {
			super(name, protocol);
		}
	}

	static final class UdpServerChannelInitializer extends ChannelInitializer<Channel> {

		final TransportConfig config;
		final ChannelOperations.OnSetup channelOperationsProvider;
		final ConnectionObserver connectionObserver;
		final SocketAddress remoteAddress;

		UdpServerChannelInitializer(TransportConfig config, ChannelOperations.OnSetup channelOperationsProvider,
				ConnectionObserver connectionObserver, @Nullable SocketAddress remoteAddress) {
			this.config = config;
			this.channelOperationsProvider = channelOperationsProvider;
			this.connectionObserver = connectionObserver;
			this.remoteAddress = remoteAddress;
		}

		@Override
		protected void initChannel(Channel ch) {
			ChannelPipeline pipeline = ch.pipeline();

			LoggingHandler loggingHandler = config.loggingHandler();
			if (loggingHandler != null) {
				pipeline.addFirst(NettyPipeline.LoggingHandler, loggingHandler);
			}

			ChannelMetricsRecorder channelMetricsRecorder = config.metricsRecorder();
			if (channelMetricsRecorder != null) {
				pipeline.addFirst(NettyPipeline.ChannelMetricsHandler,
						new ChannelMetricsHandler(channelMetricsRecorder, remoteAddress, false));

				ByteBufAllocator alloc = ch.alloc();
				if (alloc instanceof PooledByteBufAllocator) {
					ByteBufAllocatorMetrics.INSTANCE.registerMetrics("pooled", ((PooledByteBufAllocator) alloc).metric());
				}
				else if (alloc instanceof UnpooledByteBufAllocator) {
					ByteBufAllocatorMetrics.INSTANCE.registerMetrics("unpooled", ((UnpooledByteBufAllocator) alloc).metric());
				}
			}

			ChannelOperations.addReactiveBridge(ch, channelOperationsProvider, connectionObserver);

			pipeline.remove(this);

			if (log.isDebugEnabled()) {
				log.debug(format(ch, "Initialized pipeline {}"), pipeline.toString());
			}
		}
	}

	static final class UdpServerDoOn implements ConnectionObserver {

		final Consumer<? super Connection> onBound;
		final Consumer<? super Connection> onUnbound;

		UdpServerDoOn(@Nullable Consumer<? super Connection> onBound,
				@Nullable Consumer<? super Connection> onUnbound) {
			this.onBound = onBound;
			this.onUnbound = onUnbound;
		}

		@Override
		public void onStateChange(Connection connection, State newState) {
			if (onBound != null && newState == State.CONFIGURED) {
				onBound.accept(connection);
				return;
			}
			if (onUnbound != null && newState == State.DISCONNECTING) {
				connection.onDispose(() -> onUnbound.accept(connection));
			}
		}
	}
}
