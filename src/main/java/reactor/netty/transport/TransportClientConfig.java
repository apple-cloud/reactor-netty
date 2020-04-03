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
package reactor.netty.transport;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.logging.LoggingHandler;
import io.netty.resolver.AddressResolverGroup;
import io.netty.resolver.DefaultAddressResolverGroup;
import reactor.netty.Connection;
import reactor.netty.ConnectionObserver;
import reactor.netty.NettyPipeline;
import reactor.netty.channel.ByteBufAllocatorMetrics;
import reactor.netty.channel.ChannelMetricsHandler;
import reactor.netty.channel.ChannelMetricsRecorder;
import reactor.netty.channel.ChannelOperations;
import reactor.netty.resources.ConnectionProvider;
import reactor.util.Logger;
import reactor.util.Loggers;

import javax.annotation.Nullable;

import static reactor.netty.ReactorNetty.format;

/**
 * Encapsulate all necessary configuration for client transport.
 *
 * @param <CONF> Configuration implementation
 * @param <CONN> Connection implementation
 * @author Stephane Maldini
 * @author Violeta Georgieva
 * @since 1.0.0
 */
public abstract class TransportClientConfig<CONF extends TransportConfig, CONN extends Connection> extends TransportConfig {

	/**
	 * Return the {@link ConnectionProvider}
	 *
	 * @return the {@link ConnectionProvider}
	 */
	public final ConnectionProvider connectionProvider() {
		return connectionProvider;
	}

	/**
	 * Return the configured callback
	 *
	 * @return the configured callback
	 */
	@Nullable
	public final Consumer<? super CONF> doOnConnect() {
		return doOnConnect;
	}

	/**
	 * Return the configured callback
	 *
	 * @return the configured callback
	 */
	@Nullable
	public final Consumer<? super CONN> doOnConnected() {
		return doOnConnected;
	}

	/**
	 * Return the configured callback
	 *
	 * @return the configured callback
	 */
	@Nullable
	public final Consumer<? super CONN> doOnDisconnected() {
		return doOnDisconnected;
	}

	/**
	 * Return the remote configured {@link SocketAddress}
	 *
	 * @return the remote configured {@link SocketAddress}
	 */
	public final Supplier<? extends SocketAddress> remoteAddress() {
		return remoteAddress;
	}

	/**
	 * Return the {@link AddressResolverGroup}
	 *
	 * @return the {@link AddressResolverGroup}
	 */
	public final AddressResolverGroup<?> resolver() {
		return resolver;
	}


	// Package private creators

	final ConnectionProvider connectionProvider;

	Consumer<? super CONF>            doOnConnect;
	Consumer<? super CONN>            doOnConnected;
	Consumer<? super CONN>            doOnDisconnected;
	Supplier<? extends SocketAddress> remoteAddress;
	AddressResolverGroup<?>           resolver;

	protected TransportClientConfig(ConnectionProvider connectionProvider, Map<ChannelOption<?>, ?> options,
			Supplier<? extends SocketAddress> remoteAddress) {
		super(options);
		this.connectionProvider = Objects.requireNonNull(connectionProvider, "connectionProvider");
		this.remoteAddress = Objects.requireNonNull(remoteAddress, "remoteAddress");
		this.resolver = DefaultAddressResolverGroup.INSTANCE;
	}

	protected TransportClientConfig(TransportClientConfig<CONF, CONN> parent) {
		super(parent);
		this.connectionProvider = parent.connectionProvider;
		this.doOnConnect = parent.doOnConnect;
		this.doOnConnected = parent.doOnConnected;
		this.doOnDisconnected = parent.doOnDisconnected;
		this.remoteAddress = parent.remoteAddress;
		this.resolver = parent.resolver;
	}

	@Override
	protected ChannelInitializer<Channel> channelInitializer(ChannelOperations.OnSetup channelOperationsProvider,
			ConnectionObserver connectionObserver, @Nullable SocketAddress remoteAddress) {
		return new TransportClientChannelInitializer(this, channelOperationsProvider, connectionObserver, remoteAddress);
	}

	static final class TransportClientChannelInitializer extends ChannelInitializer<Channel> {

		final TransportConfig config;
		final ChannelOperations.OnSetup channelOperationsProvider;
		final ConnectionObserver connectionObserver;
		final SocketAddress remoteAddress;

		TransportClientChannelInitializer(TransportConfig config, ChannelOperations.OnSetup channelOperationsProvider,
				ConnectionObserver connectionObserver, SocketAddress remoteAddress) {
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

	static final Logger log = Loggers.getLogger(TransportClientConfig.class);
}