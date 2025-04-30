package com.yourcompany.minimalclient;

import static org.hyperledger.besu.ethereum.p2p.discovery.internal.PacketType.FIND_NEIGHBORS;
import static org.hyperledger.besu.ethereum.p2p.discovery.internal.PacketType.PING;
import static org.hyperledger.besu.ethereum.p2p.discovery.internal.PacketType.PONG;

import io.vertx.core.buffer.Buffer;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt64;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.cryptoservices.KeyPairSecurityModule;
import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.ethereum.p2p.discovery.DiscoveryPeer;
import org.hyperledger.besu.ethereum.p2p.discovery.Endpoint;
import org.hyperledger.besu.ethereum.p2p.discovery.PeerDiscoveryPacketDecodingException;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.DaggerPacketPackage;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.Packet;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.PacketDeserializer;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.PacketFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.PacketSerializer;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.findneighbors.FindNeighborsPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.neighbors.NeighborsPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.ping.PingPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.ping.PingPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.pong.PongPacketData;
import org.hyperledger.besu.ethereum.p2p.discovery.internal.packet.pong.PongPacketDataFactory;
import org.hyperledger.besu.ethereum.p2p.peers.EnodeURLImpl;
import org.hyperledger.besu.plugin.data.EnodeURL;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A minimal Ethereum P2P discovery (Discv5) client.
 * This client can bind to a UDP port, generate a temporary node identity,
 * connect to specified bootstrap nodes, and perform basic discovery operations
 * like PING, PONG, FINDNODE, and NEIGHBORS processing.
 * It implements AutoCloseable for resource cleanup.
 */
@SuppressWarnings("LoggingSimilarMessage")
@Slf4j
public class MinimalClient implements AutoCloseable {

  /*
   * Constants
   */
  private static final int RECEIVE_BUFFER_SIZE = 1_600;
  private static final long PING_TIMEOUT_SECONDS = 10;
  private static final String DEFAULT_BIND_HOST = "0.0.0.0";
  private static final int DEFAULT_LISTEN_PORT = 30303;
  private static final String DEFAULT_ADVERTISED_IP = "127.0.0.1";
  private static final long MAIN_SLEEP_DURATION_MS = 20_000;
  private static final long FIND_NODE_DELAY_SECONDS = 2;
  private static final long SHUTDOWN_TIMEOUT_SECONDS = 5;
  private static final long RECEIVER_JOIN_TIMEOUT_MS = 2_000;
  private static final int SOCKET_TIMEOUT_MS = 1_000;

  /*
   * Dependencies
   */
  private final NodeKey nodeKey;
  private final PacketFactory packetFactory;
  private final PacketSerializer packetSerializer;
  private final PacketDeserializer packetDeserializer;
  private final PingPacketDataFactory pingPacketDataFactory;
  private final PongPacketDataFactory pongPacketDataFactory;
  private final FindNeighborsPacketDataFactory findNeighborsPacketDataFactory;

  /*
   * State
   */
  private DatagramSocket clientSocket;
  private Thread receiverThread;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private EnodeURL localEnode;
  private final ScheduledExecutorService scheduler = createScheduler();
  private final Map<Bytes, PeerInfo> knownPeers = new ConcurrentHashMap<>();
  private final Map<Bytes, PendingPongInfo> pendingPongs = new ConcurrentHashMap<>();

  /*
   * Records for State/Data Transfer
   */
  private enum PeerState { KNOWN, PING_SENT, BONDED, FAILED }
  private record PeerInfo(Bytes id, EnodeURL enode, PeerState state, long lastContact) {
    PeerInfo withState(PeerState newState) {
      return new PeerInfo(id, enode, newState, System.currentTimeMillis());
    }
    PeerInfo touch() {
      return new PeerInfo(id, enode, state, System.currentTimeMillis());
    }
  }
  private record PendingPongInfo(Bytes peerId, ScheduledFuture<?> timeoutTask) {}
  private record CliArgs(List<String> bootstrapNodes, String advertisedIp, int listenPort) {}


  public MinimalClient() {
    this.nodeKey = generateNodeKey();
    log.info("Generated temporary Node ID: {}", formatNodeId(this.nodeKey.getPublicKey().getEncodedBytes()));

    // Initialize Besu packet handling components (Dependencies)
    val packetPackage = DaggerPacketPackage.create();
    this.packetFactory = packetPackage.packetFactory();
    this.packetSerializer = packetPackage.packetSerializer();
    this.packetDeserializer = packetPackage.packetDeserializer();
    this.pingPacketDataFactory = packetPackage.pingPacketDataFactory();
    this.pongPacketDataFactory = packetPackage.pongPacketDataFactory();
    this.findNeighborsPacketDataFactory = packetPackage.findNeighborsPacketDataFactory();
  }

  private static NodeKey generateNodeKey() {
    val signatureAlgorithm = SignatureAlgorithmFactory.getInstance();
    val keyPair = signatureAlgorithm.generateKeyPair();
    return new NodeKey(new KeyPairSecurityModule(keyPair));
  }

  public static void main(String[] args) {
    val cliArgs = parseCliArgs(args);
    logCliArgs(cliArgs);

    if (cliArgs.bootstrapNodes().isEmpty()) {
      log.warn("No bootstrap nodes provided. Client will start but may not discover peers.");
    }

    try (val client = new MinimalClient()) {
      runClient(client, cliArgs);
    } catch (InterruptedException e) {
      log.warn("Main thread interrupted.");
      Thread.currentThread().interrupt();
    } catch (Exception e) {
      log.error("An unexpected error occurred during execution: {}", e.getMessage(), e);
      System.exit(1);
    } finally {
      log.info("Shutdown complete.");
    }
  }

  private static void runClient(MinimalClient client, CliArgs cliArgs) throws InterruptedException {
    if (!client.startNetwork(cliArgs.advertisedIp(), cliArgs.listenPort())) {
      log.error("Minimal Client failed to start its network. Exiting.");
      System.exit(1);
    }

    val bootstrapEnodes = parseBootstrapNodes(cliArgs.bootstrapNodes());
    if (bootstrapEnodes.isEmpty() && !cliArgs.bootstrapNodes().isEmpty()) {
      log.error("Failed to parse any valid bootstrap nodes. Exiting.");
      System.exit(1);
    }

    bootstrapEnodes.forEach(enode -> {
      client.addKnownPeer(enode);
      client.startDiscoveryProcess(enode);
    });

    log.info("Minimal Client is running. Waiting {} ms...", MAIN_SLEEP_DURATION_MS);
    Thread.sleep(MAIN_SLEEP_DURATION_MS);

    client.logFinalPeerStates();
  }

  private static List<EnodeURL> parseBootstrapNodes(List<String> nodeStrings) {
    return nodeStrings.stream()
      .map(MinimalClient::parseAndValidateEnode)
      .filter(Optional::isPresent)
      .map(Optional::get)
      .toList();
  }

  private static Optional<EnodeURL> parseAndValidateEnode(String enodeUrlString) {
    try {
      val targetEnodeUrl = EnodeURLImpl.fromString(enodeUrlString);
      log.debug("Parsed Enode URL: {}", targetEnodeUrl);

      if (targetEnodeUrl.getDiscoveryPort().isPresent()) {
        return Optional.of(targetEnodeUrl);
      }

      log.warn("EnodeURL {} missing discovery port, attempting inference from listening port.", formatNodeId(targetEnodeUrl.getNodeId()));
      val discoveryPort = targetEnodeUrl.getListeningPort().orElseThrow(() ->
        new IllegalArgumentException("EnodeURL has neither discovery nor listening port specified.")
      );

      val updatedEnode = EnodeURLImpl.builder()
        .configureFromEnode(targetEnodeUrl)
        .discoveryPort(discoveryPort)
        .build();
      log.info("Inferred discovery port for {}: {}", formatNodeId(updatedEnode.getNodeId()), updatedEnode);
      return Optional.of(updatedEnode);

    } catch (IllegalArgumentException e) {
      log.error("Invalid EnodeURL format or missing port information in '{}': {}", enodeUrlString, e.getMessage());
      return Optional.empty();
    } catch (Exception e) {
      log.error("Failed to parse EnodeURL '{}': {}", enodeUrlString, e.getMessage(), e);
      return Optional.empty();
    }
  }

  private void addKnownPeer(EnodeURL enode) {
    knownPeers.computeIfAbsent(enode.getNodeId(),
      id -> new PeerInfo(id, enode, PeerState.KNOWN, 0));
  }

  private void logFinalPeerStates() {
    log.info("--- Final Peer States ({} known) ---", knownPeers.size());
    knownPeers.forEach((id, info) ->
      log.info("  - {}: State={}, Enode={}", formatNodeId(id), info.state(), info.enode())
    );
  }

  public boolean startNetwork(String advertisedIp, int port) {
    if (running.get()) {
      log.warn("Client network already started.");
      return true;
    }

    try {
      clientSocket = new DatagramSocket(new InetSocketAddress(DEFAULT_BIND_HOST, port));
      clientSocket.setSoTimeout(SOCKET_TIMEOUT_MS);
      val actualPort = clientSocket.getLocalPort();
      log.info("UDP listener started on {}:{}", DEFAULT_BIND_HOST, actualPort);

      this.localEnode = EnodeURLImpl.builder()
        .nodeId(nodeKey.getPublicKey().getEncodedBytes())
        .ipAddress(advertisedIp)
        .discoveryPort(actualPort)
        .listeningPort(0)
        .build();
      log.info("Local EnodeURL: {}", this.localEnode);

      running.set(true);
      receiverThread = Thread.ofVirtual().name("minimal-client-receiver").start(this::receiveLoop);
      log.info("Packet receiver thread started.");
      return true;

    } catch (SocketException e) {
      log.error("Failed to bind UDP socket to {}:{}: {}", DEFAULT_BIND_HOST, port, e.getMessage(), e);
    } catch (Exception e) {
      log.error("Failed to start network components: {}", e.getMessage(), e);
    }

    closeSocket();
    this.localEnode = null;
    running.set(false);
    return false;
  }

  public void stopNetwork() {
    if (!running.compareAndSet(true, false)) {
      log.debug("Network stop requested but already stopping/stopped.");
      return;
    }
    log.info("Stopping network components...");

    joinReceiverThread();
    closeSocket();
    receiverThread = null;
    log.info("Network components stopped.");
  }

  private void joinReceiverThread() {
    if (receiverThread != null && receiverThread.isAlive()) {
      log.debug("Waiting for receiver thread to finish...");
      try {
        receiverThread.join(RECEIVER_JOIN_TIMEOUT_MS);
        if (receiverThread.isAlive()) {
          log.warn("Receiver thread did not exit gracefully after {} ms.", RECEIVER_JOIN_TIMEOUT_MS);
        } else {
          log.debug("Receiver thread finished.");
        }
      } catch (InterruptedException e) {
        log.warn("Interrupted while waiting for receiver thread to stop.");
        Thread.currentThread().interrupt();
      }
    }
  }

  private void closeSocket() {
    if (clientSocket != null && !clientSocket.isClosed()) {
      log.debug("Closing UDP socket on port {}", clientSocket.getLocalPort());
      clientSocket.close();
    }
    clientSocket = null;
  }

  @Override
  public void close() {
    log.info("Shutting down MinimalClient...");
    stopNetwork();
    shutdownScheduler();
    log.info("MinimalClient shutdown complete.");
  }

  private void shutdownScheduler() {
    log.debug("Shutting down scheduler...");
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(SHUTDOWN_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
        log.warn("Scheduler did not terminate gracefully after {} seconds, forcing shutdown.", SHUTDOWN_TIMEOUT_SECONDS);
        scheduler.shutdownNow();
      } else {
        log.debug("Scheduler shut down successfully.");
      }
    } catch (InterruptedException e) {
      log.warn("Interrupted while waiting for scheduler shutdown.");
      scheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  public void startDiscoveryProcess(EnodeURL targetEnodeUrl) {
    if (localEnode == null) {
      log.error("Cannot start discovery process towards {}, local EnodeURL is not determined.", formatNodeId(targetEnodeUrl.getNodeId()));
      return;
    }
    log.info("Starting discovery towards: {}", formatNodeId(targetEnodeUrl.getNodeId()));

    val targetPeerInfo = knownPeers.computeIfAbsent(targetEnodeUrl.getNodeId(),
      id -> new PeerInfo(id, targetEnodeUrl, PeerState.KNOWN, System.currentTimeMillis()));

    initiatePingIfNeeded(targetPeerInfo);
    scheduleFindNeighbors(targetEnodeUrl);
  }

  private void initiatePingIfNeeded(PeerInfo targetPeerInfo) {
    if (targetPeerInfo.state() == PeerState.KNOWN || targetPeerInfo.state() == PeerState.FAILED) {
      try {
        val pingPacket = createPingPacket(localEnode, targetPeerInfo.enode());
        sendPingAndUpdateState(pingPacket, targetPeerInfo);
      } catch (Exception e) {
        log.error("Failed PING initiation to {}: {}", formatNodeId(targetPeerInfo.id()), e.getMessage(), e);
        updatePeerState(targetPeerInfo.id(), PeerState.FAILED);
      }
    } else {
      log.debug("Skipping PING to {} (state: {})", formatNodeId(targetPeerInfo.id()), targetPeerInfo.state());
    }
  }

  private void scheduleFindNeighbors(EnodeURL targetEnodeUrl) {
    log.debug("Scheduling FINDNODE request to {} in {} seconds...", formatNodeId(targetEnodeUrl.getNodeId()), FIND_NODE_DELAY_SECONDS);
    scheduler.schedule(() -> {
      val currentTargetInfo = knownPeers.get(targetEnodeUrl.getNodeId());
      if (currentTargetInfo == null) {
        log.warn("Target peer {} not found when trying to send scheduled FINDNODE.", formatNodeId(targetEnodeUrl.getNodeId()));
        return;
      }

      if (currentTargetInfo.state() == PeerState.BONDED) {
        sendFindNeighborsToBondedPeer(currentTargetInfo);
      } else {
        log.debug("Skipping scheduled FINDNODE to peer {} (state: {})", formatNodeId(currentTargetInfo.id()), currentTargetInfo.state());
      }
    }, FIND_NODE_DELAY_SECONDS, TimeUnit.SECONDS);
  }

  private void sendFindNeighborsToBondedPeer(PeerInfo targetInfo) {
    try {
      log.info("Sending FINDNODE request to {}", formatNodeId(targetInfo.id()));
      val findNodePacket = createFindNeighborsPacket(this.localEnode.getNodeId());
      sendPacket(findNodePacket, targetInfo.enode());
    } catch (Exception e) {
      log.error("Failed to send FINDNODE to {}: {}", formatNodeId(targetInfo.id()), e.getMessage(), e);
    }
  }

  private void receiveLoop() {
    log.info("Packet receiver loop started.");
    val receiveBuffer = new byte[RECEIVE_BUFFER_SIZE]; // Use val for effectively final byte array

    while (running.get()) {
      // DatagramPacket is reused, so cannot be final (val)
      var udpPacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
      try {
        clientSocket.receive(udpPacket);
        log.trace("Raw packet received from {}:{} ({} bytes)", udpPacket.getAddress(), udpPacket.getPort(), udpPacket.getLength());
        processReceivedPacket(udpPacket);
      } catch (SocketTimeoutException ignored) {
        // Normal timeout
      } catch (SocketException e) {
        handleSocketExceptionInLoop(e);
        break;
      } catch (IOException e) {
        log.error("IOException in receive loop: {}", e.getMessage(), e);
        running.set(false);
        break;
      } catch (Exception e) {
        log.error("Unexpected error in receive loop: {}", e.getMessage(), e);
        running.set(false);
        break;
      }
    }
    log.info("Packet receiver loop finished.");
  }

  private void handleSocketExceptionInLoop(SocketException e) {
    if (running.get()) {
      log.warn("SocketException in receive loop (socket closed?): {}", e.getMessage());
    } else {
      log.debug("SocketException after stop signal (expected): {}", e.getMessage());
    }
    running.set(false);
  }

  private void processReceivedPacket(DatagramPacket udpPacket) {
    val receivedData = Arrays.copyOf(udpPacket.getData(), udpPacket.getLength());
    val senderAddress = new InetSocketAddress(udpPacket.getAddress(), udpPacket.getPort());

    try {
      val packet = packetDeserializer.decode(Buffer.buffer(receivedData));
      handleIncomingPacket(packet, senderAddress);
    } catch (PeerDiscoveryPacketDecodingException e) {
      log.debug("Discarding invalid discovery packet from {}: {}", senderAddress, e.getMessage());
    } catch (Exception e) {
      log.error("Error processing packet from {}: {}", senderAddress, e.getMessage(), e);
    }
  }

  private void handleIncomingPacket(Packet packet, InetSocketAddress sender) {
    log.debug("Received {} packet from {}", packet.getType(), sender);

    switch (packet.getType()) {
      case PONG -> handlePongPacket(packet, sender);
      case NEIGHBORS -> handleNeighborsPacket(packet, sender);
      case PING -> handlePingPacket(packet, sender);
      default -> log.trace("Received unhandled packet type {} from {}", packet.getType(), sender);
    }
  }

  private void handlePingPacket(Packet pingPacket, InetSocketAddress sender) {
    val pingDataOpt = pingPacket.getPacketData(PingPacketData.class);
    if (pingDataOpt.isEmpty()) {
      log.warn("Received PING packet with invalid data from {}", sender);
      return;
    }

    val receivedPingHash = pingPacket.getHash();
    val ourEnrSeq = 0L;
    val replyToEndpoint = new Endpoint(sender.getAddress().getHostAddress(), sender.getPort(), Optional.empty());

    try {
      val pongData = pongPacketDataFactory.create(replyToEndpoint, receivedPingHash, UInt64.valueOf(ourEnrSeq));
      val pongReplyPacket = packetFactory.create(PONG, pongData, nodeKey);
      log.info("Responding PONG to PING from {}", sender);
      sendPacket(pongReplyPacket, sender);
    } catch (Exception e) {
      log.error("Failed to create/send PONG reply to {}: {}", sender, e.getMessage(), e);
    }
  }

  private void handlePongPacket(Packet pongPacket, InetSocketAddress sender) {
    val pongDataOpt = pongPacket.getPacketData(PongPacketData.class);
    if (pongDataOpt.isEmpty()) {
      log.warn("Received PONG packet with invalid data from {}", sender);
      return;
    }

    val pongData = pongDataOpt.get();
    val pingHash = pongData.getPingHash();

    val pendingInfo = pendingPongs.remove(pingHash);

    if (pendingInfo != null) {
      log.info("Received PONG from {} (for ping hash {})", formatNodeId(pendingInfo.peerId()), formatNodeId(pingHash));
      pendingInfo.timeoutTask().cancel(false);

      updatePeerState(pendingInfo.peerId(), currentInfo -> {
        if (currentInfo.state() == PeerState.PING_SENT) {
          log.info("Peer {} transitioned to BONDED", formatNodeId(currentInfo.id()));
          return currentInfo.withState(PeerState.BONDED);
        } else {
          log.warn("Received PONG for peer {} but its state was {} (expected PING_SENT). Ignoring PONG for state update.", formatNodeId(currentInfo.id()), currentInfo.state());
          return currentInfo;
        }
      });
    } else {
      log.debug("Received unexpected PONG (hash {}) from {}", formatNodeId(pingHash), sender);
      val senderId = pongPacket.getNodeId();
      knownPeers.computeIfPresent(senderId, (id, info) -> info.touch());
    }
  }

  private void handleNeighborsPacket(Packet neighborsPacket, InetSocketAddress sender) {
    val neighborsDataOpt = neighborsPacket.getPacketData(NeighborsPacketData.class);
    if (neighborsDataOpt.isEmpty()) {
      log.warn("Received NEIGHBORS packet with invalid data from {}", sender);
      return;
    }

    val neighborsData = neighborsDataOpt.get();
    val neighbors = neighborsData.getNodes();
    val senderId = neighborsPacket.getNodeId();

    if (neighbors.isEmpty()) {
      log.info("Received NEIGHBORS packet from {} with 0 neighbors.", formatNodeId(senderId));
      return;
    }

    logReceivedNeighbors(senderId, neighbors);

    val ourId = nodeKey.getPublicKey().getEncodedBytes();
    // Use standard int as these are modified in the loop
    var addedCount = 0;
    var updatedCount = 0;

    for (val neighbor : neighbors) { // Can use val for loop variable
      if (neighbor.getId().equals(ourId)) {
        continue;
      }

      val neighborEnodeOpt = buildEnodeUrlFromDiscoveryPeer(neighbor);
      if (neighborEnodeOpt.isEmpty()) {
        log.warn("Could not obtain valid EnodeURL for neighbor {}, skipping.", formatNodeId(neighbor.getId()));
        continue;
      }
      val neighborEnode = neighborEnodeOpt.get();

      val newPeerInfo = new PeerInfo(neighbor.getId(), neighborEnode, PeerState.KNOWN, System.currentTimeMillis());
      val existingInfo = knownPeers.putIfAbsent(neighbor.getId(), newPeerInfo);

      if (existingInfo != null) {
        knownPeers.computeIfPresent(neighbor.getId(), (id, info) -> info.touch());
        updatedCount++;
        log.trace("Updated known peer {}", formatNodeId(neighbor.getId()));
      } else {
        addedCount++;
        log.debug("Discovered new peer {}", formatNodeId(neighbor.getId()));
      }
    }

    logNeighborUpdateSummary(senderId, addedCount, updatedCount);
  }

  private void logReceivedNeighbors(Bytes senderId, List<DiscoveryPeer> neighbors) {
    log.info("Received {} neighbors from {}: {}",
      neighbors.size(), formatNodeId(senderId),
      neighbors.stream()
        .map(p -> formatNodeId(p.getId()))
        .collect(Collectors.joining(", ")));
  }

  private Optional<EnodeURL> buildEnodeUrlFromDiscoveryPeer(DiscoveryPeer neighbor) {
    if (neighbor.getEnodeURL() != null) {
      return Optional.of(neighbor.getEnodeURL());
    }

    log.trace("Neighbor peer {} missing EnodeURL, attempting construction.", formatNodeId(neighbor.getId()));
    try {
      val endpoint = neighbor.getEndpoint();
      val listeningPort = endpoint.getTcpPort().orElse(endpoint.getUdpPort());
      val constructedEnode = EnodeURLImpl.builder()
        .nodeId(neighbor.getId())
        .ipAddress(endpoint.getHost())
        .discoveryPort(endpoint.getUdpPort())
        .listeningPort(listeningPort)
        .build();
      return Optional.of(constructedEnode);
    } catch (Exception e) {
      log.warn("Could not construct EnodeURL for neighbor {}: {}", formatNodeId(neighbor.getId()), e.getMessage());
      return Optional.empty();
    }
  }

  private void logNeighborUpdateSummary(Bytes senderId, int addedCount, int updatedCount) {
    if (addedCount > 0) {
      log.info("Added {} new peers from NEIGHBORS received from {}", addedCount, formatNodeId(senderId));
    }
    if (updatedCount > 0) {
      log.debug("Updated {} existing peers from NEIGHBORS received from {}", updatedCount, formatNodeId(senderId));
    }
  }

  private void sendPacket(Packet packetToSend, EnodeURL targetEnode) {
    if (clientSocket == null || clientSocket.isClosed()) {
      log.error("Cannot send {} packet to {}, socket not ready.", packetToSend.getType(), formatNodeId(targetEnode.getNodeId()));
      return;
    }

    try {
      val datagramPacket = createDatagramPacket(packetToSend, targetEnode);
      log.debug("Sending {} packet (hash {}) to {}",
        packetToSend.getType(), formatNodeId(packetToSend.getHash()), formatNodeId(targetEnode.getNodeId()));

      clientSocket.send(datagramPacket);
      log.trace("Sent {} successfully to {}", packetToSend.getType(), formatNodeId(targetEnode.getNodeId()));

    } catch (IOException e) {
      log.error("Network error sending {} to {}: {}", packetToSend.getType(), formatNodeId(targetEnode.getNodeId()), e.getMessage());
    } catch (Exception e) {
      log.error("Failed to prepare/send {} packet to {}: {}", packetToSend.getType(), formatNodeId(targetEnode.getNodeId()), e.getMessage(), e);
    }
  }

  private void sendPacket(Packet packetToSend, InetSocketAddress recipient) {
    if (clientSocket == null || clientSocket.isClosed()) {
      log.error("Cannot send {} packet to {}, socket not ready.", packetToSend.getType(), recipient);
      return;
    }

    try {
      val packetBuffer = packetSerializer.encode(packetToSend);
      val packetBytes = packetBuffer.getBytes();
      val datagramPacket = new DatagramPacket(
        packetBytes, packetBytes.length, recipient.getAddress(), recipient.getPort());

      log.debug("Sending {} packet (hash {}) to {}",
        packetToSend.getType(), formatNodeId(packetToSend.getHash()), recipient);

      clientSocket.send(datagramPacket);
      log.trace("Sent {} successfully to {}", packetToSend.getType(), recipient);

    } catch (IOException e) {
      log.error("Network error sending {} to {}: {}", packetToSend.getType(), recipient, e.getMessage());
    } catch (Exception e) {
      log.error("Failed to prepare/send {} packet to {}: {}", packetToSend.getType(), recipient, e.getMessage(), e);
    }
  }

  private void sendPingAndUpdateState(Packet pingPacket, PeerInfo targetPeerInfo) {
    val peerId = targetPeerInfo.id();
    val targetEnodeUrl = targetPeerInfo.enode();
    val pingHash = pingPacket.getHash();

    if (clientSocket == null || clientSocket.isClosed()) {
      log.error("Cannot send PING to {}, socket not ready.", formatNodeId(peerId));
      updatePeerState(peerId, PeerState.FAILED);
      return;
    }

    try {
      updatePeerState(peerId, PeerState.PING_SENT);
      log.info("Sending PING to {} (state -> PING_SENT)", formatNodeId(peerId));

      val timeoutTask = schedulePongTimeout(pingHash, peerId);
      registerPendingPong(pingHash, peerId, timeoutTask);

      val datagramPacket = createDatagramPacket(pingPacket, targetEnodeUrl);
      clientSocket.send(datagramPacket);
      log.debug("PING (hash {}) sent successfully to {}", formatNodeId(pingHash), formatNodeId(peerId));

    } catch (IOException e) {
      log.error("Network error sending PING to {}: {}", formatNodeId(peerId), e.getMessage());
      handleSendFailure(peerId, pingHash);
    } catch (Exception e) {
      log.error("Failed to prepare/schedule PING packet for {}: {}", formatNodeId(peerId), e.getMessage(), e);
      handleSendFailure(peerId, pingHash);
    }
  }

  private void registerPendingPong(Bytes pingHash, Bytes peerId, ScheduledFuture<?> timeoutTask) {
    val previous = pendingPongs.put(pingHash, new PendingPongInfo(peerId, timeoutTask));
    if(previous != null) {
      log.warn("Replaced existing pending PONG entry for hash {}", formatNodeId(pingHash));
      previous.timeoutTask().cancel(false);
    }
    log.trace("Registered pending PONG for hash {}", formatNodeId(pingHash));
  }

  private ScheduledFuture<?> schedulePongTimeout(Bytes pingHash, Bytes peerId) {
    return scheduler.schedule(() -> {
      val timedOutInfo = pendingPongs.remove(pingHash);
      if (timedOutInfo != null) {
        if (timedOutInfo.peerId().equals(peerId)) {
          log.warn("Timeout waiting for PONG from {} (ping hash {})", formatNodeId(peerId), formatNodeId(pingHash));
          updatePeerState(peerId, currentInfo ->
            currentInfo.state() == PeerState.PING_SENT ? currentInfo.withState(PeerState.FAILED) : currentInfo
          );
        } else {
          log.warn("Timeout for PONG hash {} occurred, but associated peer ID {} differs from expected {}", formatNodeId(pingHash), formatNodeId(timedOutInfo.peerId()), formatNodeId(peerId));
        }
      }
    }, PING_TIMEOUT_SECONDS, TimeUnit.SECONDS);
  }

  private void handleSendFailure(Bytes peerId, Bytes pingHash) {
    updatePeerState(peerId, PeerState.FAILED);
    val failedInfo = pendingPongs.remove(pingHash);
    if (failedInfo != null) {
      failedInfo.timeoutTask().cancel(false);
      log.debug("Cancelled PONG timeout for failed PING to {}", formatNodeId(peerId));
    }
  }

  private Packet createPingPacket(EnodeURL localEnodeURL, EnodeURL targetEnodeUrl) {
    log.trace("Creating PING packet data for target: {}", targetEnodeUrl);
    val ourEnrSeq = 0L;
    val pingData = pingPacketDataFactory.create(
      Optional.ofNullable(localEnodeURL).map(Endpoint::fromEnode),
      Endpoint.fromEnode(targetEnodeUrl),
      UInt64.valueOf(ourEnrSeq)
    );
    try {
      val packet = packetFactory.create(PING, pingData, nodeKey);
      log.trace("Created signed PING packet with hash {}", formatNodeId(packet.getHash()));
      return packet;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create signed PING packet", e);
    }
  }

  private Packet createFindNeighborsPacket(Bytes targetNodeId) {
    log.trace("Creating FINDNODE packet data targeting neighbors of node ID: {}", formatNodeId(targetNodeId));
    val findNeighborsData = findNeighborsPacketDataFactory.create(targetNodeId);
    try {
      val packet = packetFactory.create(FIND_NEIGHBORS, findNeighborsData, nodeKey);
      log.trace("Created signed FINDNODE packet with hash {}", formatNodeId(packet.getHash()));
      return packet;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to create signed FINDNODE packet", e);
    }
  }

  private static ScheduledExecutorService createScheduler() {
    return Executors.newSingleThreadScheduledExecutor(runnable -> {
      val thread = Executors.defaultThreadFactory().newThread(runnable);
      thread.setName("MinimalClient-Scheduler-" + thread.threadId());
      thread.setDaemon(true);
      return thread;
    });
  }

  private DatagramPacket createDatagramPacket(Packet packet, EnodeURL targetEnode) throws IOException {
    val packetBuffer = packetSerializer.encode(packet);
    val packetBytes = packetBuffer.getBytes();
    val targetIp = resolveInetAddress(targetEnode.getIpAsString());
    val targetPort = targetEnode.getDiscoveryPort().orElseThrow(() ->
      new IOException("Target EnodeURL missing discovery port: " + targetEnode));
    return new DatagramPacket(packetBytes, packetBytes.length, targetIp, targetPort);
  }

  private static InetAddress resolveInetAddress(String host) throws UnknownHostException {
    return InetAddress.getByName(host);
  }

  private void updatePeerState(Bytes peerId, PeerState newState) {
    knownPeers.computeIfPresent(peerId, (id, info) -> {
      if (info.state() != newState) {
        log.debug("Peer {} state change: {} -> {}", formatNodeId(id), info.state(), newState);
        return info.withState(newState);
      }
      return info;
    });
  }

  private void updatePeerState(Bytes peerId, java.util.function.UnaryOperator<PeerInfo> updateFunction) {
    knownPeers.computeIfPresent(peerId, (id, info) -> {
      val updatedInfo = updateFunction.apply(info);
      if (!Objects.equals(info.state(), updatedInfo.state())) {
        log.debug("Peer {} state change: {} -> {}", formatNodeId(id), info.state(), updatedInfo.state());
      }
      return updatedInfo;
    });
  }

  private static String formatNodeId(Bytes nodeId) {
    if (nodeId == null) return "<null>";
    val len = Math.min(nodeId.size(), 8);
    return nodeId.slice(0, len).toHexString() + "...";
  }

  private static CliArgs parseCliArgs(String[] args) {
    // Use var for mutable variables assigned in loop/conditional
    var bootstrapNodes = Collections.<String>emptyList();
    var advertisedIp = DEFAULT_ADVERTISED_IP;
    var listenPort = DEFAULT_LISTEN_PORT;

    for (int i = 0; i < args.length; i++) {
      val arg = args[i]; // Use val for loop variable if not reassigned inside
      switch (arg) {
        case "--advertised-ip" -> {
          if (i + 1 < args.length && !args[i+1].startsWith("--")) {
            advertisedIp = args[++i];
          } else {
            exitWithError("Missing value for --advertised-ip argument");
          }
        }
        case "--listen-port" -> {
          if (i + 1 < args.length && !args[i+1].startsWith("--")) {
            try {
              listenPort = Integer.parseInt(args[++i]);
            } catch (NumberFormatException e) {
              exitWithError("Invalid value for --listen-port argument: " + args[i]);
            }
          } else {
            exitWithError("Missing value for --listen-port argument");
          }
        }
        case "--bootstrap-nodes" -> {
          if (i + 1 < args.length && !args[i+1].startsWith("--")) {
            bootstrapNodes = Stream.of(args[++i].split(","))
              .map(String::trim)
              .filter(s -> !s.isEmpty())
              .toList();
          } else {
            exitWithError("Missing value for --bootstrap-nodes argument");
          }
        }
        default -> {
          if (arg.startsWith("--")) {
            log.warn("Ignoring unknown option: {}", arg);
          } else {
            log.warn("Ignoring unexpected positional argument: {}", arg);
          }
        }
      }
    }
    return new CliArgs(bootstrapNodes, advertisedIp, listenPort);
  }

  private static void logCliArgs(CliArgs cliArgs) {
    log.info("--- Configuration ---");
    log.info("Advertised IP: {}", cliArgs.advertisedIp());
    log.info("Listen Port: {}", cliArgs.listenPort());
    log.info("Bootstrap Nodes: {}", cliArgs.bootstrapNodes().isEmpty() ? "<none>" : String.join(", ", cliArgs.bootstrapNodes()));
    log.info("---------------------");
  }

  private static void exitWithError(String message) {
    log.error("Argument Error: {}", message);
    log.error("Usage: MinimalClient --bootstrap-nodes <enode1[,enode2,...]> [--advertised-ip <ip>] [--listen-port <port>]");
    System.exit(1);
  }
}
