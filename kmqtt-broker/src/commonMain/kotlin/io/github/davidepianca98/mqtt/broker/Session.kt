package io.github.davidepianca98.mqtt.broker

import io.github.davidepianca98.currentTimeMillis
import io.github.davidepianca98.mqtt.Will
import io.github.davidepianca98.mqtt.MQTTVersion
import io.github.davidepianca98.mqtt.packets.MQTTPacket
import io.github.davidepianca98.mqtt.packets.Qos
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubrel
import io.github.davidepianca98.mqtt.packets.mqttv4.MQTT4Publish
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Properties
import io.github.davidepianca98.mqtt.packets.mqttv5.MQTT5Publish
import io.github.davidepianca98.mqtt.packets.mqttv5.ReasonCode
import io.github.davidepianca98.mqtt.broker.InflightState

public class Session(
    public var clientConnection: ClientConnection?,
    override val clientId: String,
    override var sessionExpiryInterval: UInt,
    override var will: Will?,
    private val persist: (clientId: String, session: Session) -> Unit,
    private val propagateUpdate: (session: Session) -> Unit,
    private val persistInflight: ((clientId: String, state: InflightState) -> Unit)? = null,
    inflightState: InflightState? = null
) : ISession {

    private val inflightFlushIntervalMs = 5_000L
    override var connected: Boolean = false // true only after sending CONNACK
        set(value) {
            if (value) {
                field = value
                sessionDisconnectedTimestamp = null
                persist()
            } else {
                if (connected) {
                    field = value
                    clientConnection = null
                    if (sessionDisconnectedTimestamp == null) {
                        sessionDisconnectedTimestamp = currentTimeMillis()
                    }
                }
                persist()
            }
            markInflightChanged(force = !value)
            propagateUpdate(this)
        }
    override var mqttVersion: MQTTVersion = MQTTVersion.MQTT3_1_1
    override var sessionDisconnectedTimestamp: Long? = null

    private var packetIdentifier = 1u

    // QoS 1 and QoS 2 messages which have been sent to the Client, but have not been completely acknowledged
    private val pendingAcknowledgeMessages = mutableMapOf<UInt, MQTTPublish>()
    private val pendingAcknowledgePubrel = mutableMapOf<UInt, MQTTPubrel>()

    // QoS 1 and QoS 2 messages pending transmission to the Client
    private val pendingSendMessages = mutableMapOf<UInt, MQTTPublish>()

    // QoS 2 messages which have been received from the Client that have not been completely acknowledged
    public val qos2ListReceived: MutableMap<UInt, MQTTPublish> = mutableMapOf()

    private var inflightDirty = false
    private var lastInflightPersist = 0L

    init {
        inflightState?.let { restoreInflight(it) }
        persist()
    }

    private fun persist() {
        this.persist(clientId, this)
        flushInflightIfNeeded()
    }

    private fun restoreInflight(state: InflightState) {
        packetIdentifier = state.packetIdentifier
        pendingSendMessages.putAll(state.pendingSendMessages)
        pendingAcknowledgeMessages.putAll(state.pendingAcknowledgeMessages)
        pendingAcknowledgePubrel.putAll(state.pendingAcknowledgePubrel)
        qos2ListReceived.putAll(state.qos2ListReceived)
        markInflightChanged(force = true)
    }

    private fun snapshotInflight(): InflightState = InflightState(
        pendingSendMessages.toMap(),
        pendingAcknowledgeMessages.toMap(),
        pendingAcknowledgePubrel.toMap(),
        qos2ListReceived.toMap(),
        packetIdentifier
    )

    private fun markInflightChanged(force: Boolean = false) {
        inflightDirty = true
        flushInflightIfNeeded(force)
    }

    internal fun inflightChanged(force: Boolean = false) {
        markInflightChanged(force)
    }

    private fun flushInflightIfNeeded(force: Boolean = false) {
        val saver = persistInflight ?: return
        if (!inflightDirty && !force) return
        val now = currentTimeMillis()
        if (!force && now - lastInflightPersist < inflightFlushIntervalMs) {
            return
        }
        saver(clientId, snapshotInflight())
        inflightDirty = false
        lastInflightPersist = now
    }

    internal fun flushInflight() {
        flushInflightIfNeeded(force = true)
    }

    public fun hasPendingAcknowledgeMessage(packetId: UInt): Boolean {
        return pendingAcknowledgeMessages[packetId] != null
    }

    public fun acknowledgePublish(packetId: UInt) {
        pendingAcknowledgeMessages.remove(packetId)
        persist()
        markInflightChanged()
    }

    public fun addPendingAcknowledgePubrel(packet: MQTTPubrel) {
        pendingAcknowledgePubrel[packet.packetId] = packet
        persist()
        markInflightChanged()
    }

    public fun acknowledgePubrel(packetId: UInt) {
        pendingAcknowledgePubrel.remove(packetId)
        persist()
        markInflightChanged()
    }

    internal fun sendPending(sendPacket: (packet: MQTTPacket) -> Unit) {
        val iterator = pendingSendMessages.iterator()
        while (iterator.hasNext()) {
            val packet = iterator.next().value
            if (!packet.messageExpiryIntervalExpired()) {
                packet.updateMessageExpiryInterval()
                sendPacket(packet)
                pendingAcknowledgeMessages[packet.packetId!!] = packet
                iterator.remove()
                markInflightChanged()
            }
        }
        flushInflightIfNeeded()
    }

    internal fun resendPending(sendPacket: (packet: MQTTPacket) -> Unit) {
        pendingAcknowledgeMessages.forEach {
            if (!it.value.messageExpiryIntervalExpired()) {
                it.value.updateMessageExpiryInterval()
                sendPacket(it.value.setDuplicate())
            }
        }
        pendingAcknowledgePubrel.forEach {
            sendPacket(it.value)
        }
        sendPending(sendPacket)
    }

    internal fun publish(packet: MQTTPublish) {
        if (packet.messageExpiryIntervalExpired())
            return
        // Update the expiry interval if present
        packet.updateMessageExpiryInterval()

        if (packet.qos == Qos.AT_LEAST_ONCE || packet.qos == Qos.EXACTLY_ONCE) {
            if (connected && (clientConnection?.sendQuota ?: 1u) > 0u) {
                // Fast path: send immediately and move straight to pendingAck
                pendingAcknowledgeMessages[packet.packetId!!] = packet
                persist()
                clientConnection!!.writePacket(packet)
                clientConnection!!.decrementSendQuota()
            } else {
                // Offline or quota exhausted: queue for later delivery
                pendingSendMessages[packet.packetId!!] = packet
                persist()
            }
        } else {
            if (connected) {
                clientConnection!!.writePacket(packet)
            }
        }
    }

    override fun publish(
        retain: Boolean,
        topicName: String,
        qos: Qos,
        dup: Boolean,
        properties: MQTT5Properties?,
        payload: UByteArray?
    ) {
        val packetId = if (qos >= Qos.AT_MOST_ONCE) generatePacketId() else null

        val packetTopicName =
            clientConnection?.getPublishTopicAlias(topicName, properties ?: MQTT5Properties()) ?: topicName

        val packet = if (mqttVersion == MQTTVersion.MQTT5) {
            MQTT5Publish(
                retain,
                qos,
                dup,
                packetTopicName,
                packetId,
                properties ?: MQTT5Properties(),
                payload
            )
        } else {
            MQTT4Publish(
                retain,
                qos,
                dup,
                packetTopicName,
                packetId,
                payload
            )
        }

        publish(packet)
    }

    // TODO shared subscription note:
    //  If the Server is in the process of sending a QoS 1 message to its chosen subscribing Client and the connection
    //  to that Client breaks before the Server has received an acknowledgement from the Client, the Server MAY wait for
    //  the Client to reconnect and retransmit the message to that Client. If the Client'sSession terminates before the
    //  Client reconnects, the Server SHOULD send the Application Message to another Client that is subscribed to the
    //  same Shared Subscription. It MAY attempt to send the message to another Client as soon as it loses its
    //  connection to the first Client.

    public fun generatePacketId(): UInt {
        do {
            packetIdentifier++
            if (packetIdentifier > 65535u)
                packetIdentifier = 1u
        } while (isPacketIdInUse(packetIdentifier))
        markInflightChanged()

        return packetIdentifier
    }

    public fun isPacketIdInUse(packetId: UInt): Boolean {
        if (pendingSendMessages[packetId] != null)
            return true
        if (pendingAcknowledgeMessages[packetId] != null)
            return true
        return false
    }

    override fun disconnectClientSessionTakenOver() { // TODO probably transform in remotesession if the method is called from clusterconnection
        clientConnection?.disconnect(ReasonCode.SESSION_TAKEN_OVER)
    }

    override fun checkKeepAliveExpired() {
        clientConnection?.checkKeepAliveExpired()
    }
}
