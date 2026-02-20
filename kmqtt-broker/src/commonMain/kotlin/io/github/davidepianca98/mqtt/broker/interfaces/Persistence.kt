package io.github.davidepianca98.mqtt.broker.interfaces

import io.github.davidepianca98.mqtt.broker.Session
import io.github.davidepianca98.mqtt.Subscription
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPublish
import io.github.davidepianca98.mqtt.packets.mqtt.MQTTPubrel
import io.github.davidepianca98.mqtt.broker.InflightState

public interface Persistence {

    /**
     * Get all the sessions from persistent storage
     * @return map of clientId to [Session]
     */
    public fun getAllSessions(): Map<String, Session>

    /**
     * Save session to persistent storage
     * @param clientId ID of the client
     * @param session the session that must be persisted
     */
    public fun persistSession(clientId: String, session: Session)

    /**
     * Delete a session from persistent storage
     * @param clientId ID of the client to match the session
     */
    public fun removeSession(clientId: String)

    /**
     * Get all the subscriptions from persistent storage
     * @return list of clientId to [Subscription] pairs (a client may have multiple subscriptions)
     */
    public fun getAllSubscriptions(): List<Pair<String, Subscription>>

    /**
     * Save subscription to persistent storage
     * @param clientId ID of the client
     * @param subscription the subscription that must be persisted
     */
    public fun persistSubscription(clientId: String, subscription: Subscription)

    /**
     * Delete a subscription from persistent storage
     * @param clientId ID of the client
     * @param topicFilter topicFilter of the subscription that must be removed
     */
    public fun removeSubscription(clientId: String, topicFilter: String)

    /**
     * Delete all the subscriptions of the specified client from persistent storage
     * @param clientId ID of the client
     */
    public fun removeSubscriptions(clientId: String)

    /**
     * Get all the retained messages from persistent storage
     * @return map of topicName to pair of [MQTTPublish] message and clientId
     */
    public fun getAllRetainedMessages(): Map<String, Pair<MQTTPublish, String>>

    /**
     * Save a retained message to persistent storage
     * @param message the message to be saved
     * @param clientId ID of the client
     */
    public fun persistRetained(message: MQTTPublish, clientId: String)

    /**
     * Delete the retained message associated with the specified topic name
     * @param topicName topic name
     */
    public fun removeRetained(topicName: String)

    /**
     * Save in-flight QoS state (pending deliveries/acks) for a client.
     */
    public fun persistInflight(clientId: String, state: InflightState)

    /**
     * Load in-flight QoS state for a client, or null if none persisted.
     */
    public fun getInflight(clientId: String): InflightState?

    /**
     * Clear in-flight QoS state for a client.
     */
    public fun clearInflight(clientId: String)
}
