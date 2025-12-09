package io.github.davidepianca98.mqtt.broker

import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubrel

public data class InflightState(
    val pendingSendMessages: Map<UInt, MQTTPublish>,
    val pendingAcknowledgeMessages: Map<UInt, MQTTPublish>,
    val pendingAcknowledgePubrel: Map<UInt, MQTTPubrel>,
    val qos2ListReceived: Map<UInt, MQTTPublish>,
    val packetIdentifier: UInt
)
