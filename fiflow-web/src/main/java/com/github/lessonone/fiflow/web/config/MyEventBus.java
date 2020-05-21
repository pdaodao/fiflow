package com.github.lessonone.fiflow.web.config;

import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.impl.CodecManager;
import io.vertx.core.eventbus.impl.EventBusImpl;
import io.vertx.core.eventbus.impl.MessageImpl;
import io.vertx.core.eventbus.impl.OutboundDeliveryContext;
import io.vertx.core.eventbus.impl.clustered.ClusterNodeInfo;
import io.vertx.core.eventbus.impl.clustered.ClusteredMessage;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.impl.ServerID;
import io.vertx.core.parsetools.RecordParser;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MyEventBus extends EventBusImpl {
    private static final Logger log = LoggerFactory.getLogger(MyEventBus.class);

    private static final Buffer PONG = Buffer.buffer(new byte[]{(byte) 1});
    public final ConcurrentMap<ServerID, ConnectionHolder> connections = new ConcurrentHashMap<>();
    public final EventBusOptions options;
    private final int port;
    private NetServer server;
    private ServerID serverID;
    private List<ClusterNodeInfo> subs;


    public MyEventBus(VertxInternal vertx, int port, VertxOptions options, List<ClusterNodeInfo> subs) {
        super(vertx);
        this.port = port;
        this.options = options.getEventBusOptions();
        if (subs == null) subs = new ArrayList<>();
        this.subs = subs;
    }

    public VertxInternal vertx() {
        return this.vertx;
    }

    public NetServerOptions getServerOptions() {
        return new NetServerOptions().setPort(port).setHost("127.0.0.1");
    }

    private Handler<NetSocket> getServerHandler() {
        return socket -> {
            RecordParser parser = RecordParser.newFixed(4);
            Handler<Buffer> handler = new Handler<Buffer>() {
                int size = -1;

                public void handle(Buffer buff) {
                    if (size == -1) {
                        size = buff.getInt(0);
                        parser.fixedSizeMode(size);
                    } else {
                        ClusteredMessage received = new ClusteredMessage(MyEventBus.this);

                        received.readFromWire(buff, codecManager);
                        if (metrics != null) {
                            metrics.messageRead(received.address(), buff.length());
                        }
                        parser.fixedSizeMode(4);
                        size = -1;
                        if (received.codec() == CodecManager.PING_MESSAGE_CODEC) {
                            // Just send back pong directly on connection
                            socket.write(PONG);
                        } else {
                            deliverMessageLocally(received);
                        }
                    }
                }
            };
            parser.setOutput(handler);
            socket.handler(parser);
        };
    }


    @Override
    public synchronized void start(Handler<AsyncResult<Void>> completionHandler) {
        server = vertx.createNetServer(getServerOptions());

        server.connectHandler(getServerHandler());


        server.listen(asyncResult -> {

            System.out.println("------- server listen ------- ");
            if (asyncResult.succeeded()) {
                int serverPort = asyncResult.result().actualPort();
                serverID = new ServerID(serverPort, "127.0.0.1");
                System.out.println("server start " + serverPort);
            } else {
                System.out.println("server start failed.");
            }
        });
        this.started = true;
    }

    @Override
    public void close(Handler<AsyncResult<Void>> completionHandler) {
        super.close(ar1 -> {
            if (server != null) {
                server.close(ar -> {
                    if (ar.failed()) {
                        log.error("Failed to close server", ar.cause());
                    }
                    // Close all outbound connections explicitly - don't rely on context hooks
                    for (ConnectionHolder holder : connections.values()) {
                        holder.close();
                    }
                    if (completionHandler != null) {
                        completionHandler.handle(ar);
                    }
                });
            } else {
                if (completionHandler != null) {
                    completionHandler.handle(ar1);
                }
            }
        });
    }

    @Override
    public MessageImpl createMessage(boolean send, String address, MultiMap headers, Object body, String codecName) {
        Objects.requireNonNull(address, "no null address accepted");
        MessageCodec codec = codecManager.lookupCodec(body, codecName);
        @SuppressWarnings("unchecked")
        ClusteredMessage msg = new ClusteredMessage(serverID, address, headers, body, codec, send, this);
        return msg;
    }

    @Override
    protected <T> void sendOrPub(OutboundDeliveryContext<T> sendContext) {
        if (sendContext.options.isLocalOnly()) {
            super.sendOrPub(sendContext);
        } else if (Vertx.currentContext() != sendContext.ctx) {
            for (ClusterNodeInfo nodeInfo : subs) {
                sendRemote(sendContext, nodeInfo.serverID);
            }
        } else {
            for (ClusterNodeInfo nodeInfo : subs) {
                sendRemote(sendContext, nodeInfo.serverID);
            }
        }
    }


    private void sendRemote(OutboundDeliveryContext<?> sendContext, ServerID theServerID) {
        // We need to deal with the fact that connecting can take some time and is async, and we cannot
        // block to wait for it. So we add any sends to a pending list if not connected yet.
        // Once we connect we send them.
        // This can also be invoked concurrently from different threads, so it gets a little
        // tricky
        ConnectionHolder holder = connections.get(theServerID);
        if (holder == null) {
            // When process is creating a lot of connections this can take some time
            // so increase the timeout
            holder = new ConnectionHolder(this, theServerID, options);
            ConnectionHolder prevHolder = connections.putIfAbsent(theServerID, holder);
            if (prevHolder != null) {
                // Another one sneaked in
                holder = prevHolder;
            } else {
                holder.connect();
            }
        }
        holder.writeMessage(sendContext);
    }


//    @Override
//    protected <T> void addRegistration(boolean newAddress, HandlerHolder<T> holder, Handler<AsyncResult<Void>> completionHandler) {
//        if (newAddress && subs != null && !holder.replyHandler && !holder.localOnly) {
//            // Propagate the information
//            subs.add(holder.address, nodeInfo, completionHandler);
//            ownSubs.add(holder.address);
//        } else {
//            completionHandler.handle(Future.succeededFuture());
//        }
//    }


}
