/*
 * coreMQTT Agent v1.0.0
 * Copyright (C) 2021 Amazon.com, Inc. or its affiliates.  All Rights Reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

/**
 * @file core_mqtt_agent.h
 * @brief Functions for running a coreMQTT client in a dedicated thread.
 */
#ifndef CORE_MQTT_AGENT_H
#define CORE_MQTT_AGENT_H

/* MQTT library includes. */
#include "core_mqtt.h"
#include "core_mqtt_state.h"

#include "FreeRTOS.h"
#include "queue.h"

/* Command messaging interface include. */
#include "core_mqtt_agent_message_interface.h"

/**
 * @brief The maximum number of pending acknowledgments to track for a single
 * connection.
 *
 * @note The MQTT agent tracks MQTT commands (such as PUBLISH and SUBSCRIBE) th
 * at are still waiting to be acknowledged.  MQTT_AGENT_MAX_OUTSTANDING_ACKS set
 * the maximum number of acknowledgments that can be outstanding at any one time.
 * The higher this number is the greater the agent's RAM consumption will be.
 *
 * <b>Possible values:</b> Any positive integer up to SIZE_MAX. <br>
 * <b>Default value:</b> `20`
 */
#ifndef MQTT_AGENT_MAX_OUTSTANDING_ACKS
    #define MQTT_AGENT_MAX_OUTSTANDING_ACKS    ( 20U )
#endif

/**
 * @brief Time in milliseconds that the MQTT agent task will wait in the Blocked state (so
 * not using any CPU time) for a command to arrive in its command queue before
 * exiting the blocked state so it can call MQTT_ProcessLoop().
 *
 * @note It is important MQTT_ProcessLoop() is called often if there is known
 * MQTT traffic, but calling it too often can take processing time away from
 * lower priority tasks and waste CPU time and power.
 *
 * <b>Possible values:</b> Any positive 32 bit integer. <br>
 * <b>Default value:</b> `1000`
 */
#ifndef MQTT_AGENT_MAX_EVENT_QUEUE_WAIT_TIME
    #define MQTT_AGENT_MAX_EVENT_QUEUE_WAIT_TIME    ( 1000U )
#endif

/*-----------------------------------------------------------*/

typedef struct xMQTTAgentNode
{
    void * pvDummy[ 2 ];
} MQTTAgentNode_t;

/**
 * @brief Perform any initialization the MQTT agent requires before it can
 * be used. Must be called before any other function.
 *
 * @param[in] pMqttAgentContext Pointer to struct to initialize.
 * @param[in] pMsgInterface Command interface to use for allocating and sending commands.
 * @param[in] pNetworkBuffer Pointer to network buffer to use.
 * @param[in] pTransportInterface Transport interface to use with the MQTT
 * library.  See https://www.freertos.org/network-interface.html
 * @param[in] getCurrentTimeMs Pointer to a function that returns a count value
 * that increments every millisecond.
 * @param[in] incomingCallback The callback to execute when receiving publishes.
 * @param[in] pIncomingPacketContext A pointer to a context structure defined by
 * the application writer.
 *
 * @note The @p pIncomingPacketContext context provided for the incoming publish
 * callback MUST remain in scope throughout the period that the agent task is running.
 *
 * @return Appropriate status code from MQTT_Init().
 *
 * <b>Example</b>
 * @code{c}
 *
 * // Function for obtaining a timestamp.
 * uint32_t getTimeStampMs();
 * // Callback function for receiving packets.
 * void incomingPublishCallback( MQTTAgentContext_t * pMqttAgentContext,
 *                               uint16_t packetId,
 *                               MQTTPublishInfo_t * pPublishInfo );
 * // Platform function for network send interface.
 * int32_t networkSend( NetworkContext_t * pContext, const void * pBuffer, size_t bytes );
 * // Platform for network receive interface.
 * int32_t networkRecv( NetworkContext_t * pContext, void * pBuffer, size_t bytes );
 *
 * // Platform function for Agent Message Send.
 * bool agentSendMessage( MQTTAgentMessageContext_t * pMsgCtx,
 *                        MQTTAgentCommand_t * const * pCommandToSend,
 *                        uint32_t blockTimeMs );
 * // Platform function for Agent Message Receive.
 * bool agentReceiveMessage( MQTTAgentMessageContext_t * pMsgCtx,
 *                           MQTTAgentCommand_t ** pCommandToSend,
 *                           uint32_t blockTimeMs );
 * // Platform function to Get Agent Command.
 * MQTTAgentCommand_t * getCommand( uint32_t blockTimeMs );
 * // Platform function to Release Agent Command.
 * bool releaseCommand( MQTTAgentCommand_t * pCommandToRelease );
 *
 * // Variables used in this example.
 * MQTTAgentMessageInterface_t messageInterface;
 * MQTTAgentMessageContext_t messageContext;
 * MQTTAgentContext_t agentContext;
 * TransportInterface_t transport;
 * // Buffer for storing outgoing and incoming MQTT packets.
 * MQTTFixedBuffer_t fixedBuffer;
 * uint8_t buffer[ 1024 ];
 * MQTTStatus_t status;
 *
 * // Set transport interface members.
 * transport.pNetworkContext = &someTransportContext;
 * transport.send = networkSend;
 * transport.recv = networkRecv;
 *
 * // Set agent message interface members.
 * messageInterface.pMsgCtx = &messageContext;
 * messageInterface.send = agentSendMessage;
 * messageInterface.recv = agentReceiveMessage;
 * messageInterface.getCommand = getCommand;
 * messageInterface.releaseCommand = releaseCommand;
 *
 * // Set buffer members.
 * fixedBuffer.pBuffer = buffer;
 * fixedBuffer.size = 1024;
 *
 * status = MQTTAgent_Init( &agentContext,
 *                          &messageInterface,
 *                          &networkBuffer,
 *                          &transportInterface,
 *                          stubGetTime,
 *                          stubPublishCallback,
 *                          incomingPacketContext );
 *
 * if( status == MQTTSuccess )
 * {
 *    // Do something with agentContext. The transport and message interfaces, and
 *    // fixedBuffer structs were copied into the context, so the original structs
 *    // do not need to stay in scope.
 * }
 * @endcode
 */
/* @[declare_mqtt_agent_init] */
MQTTStatus_t MQTTAgent_Init( MQTTContext_t * pContext,
                             const TransportInterface_t * pTransportInterface,
                             MQTTGetCurrentTimeFunc_t getTimeFunction,
                             const MQTTFixedBuffer_t * pNetworkBuffer,
							 UBaseType_t uxMQTTAgentPriority );
/* @[declare_mqtt_agent_init] */

/**
 * @brief Add a command to call MQTT_Subscribe() for an MQTT connection.
 *
 * @param[in] pMqttAgentContext The MQTT agent to use.
 * @param[in] pSubscriptionArgs Struct describing topic to subscribe to.
 * @param[in] pCommandInfo The information pertaining to the command, including:
 *  - cmdCompleteCallback Optional callback to invoke when the command completes.
 *  - pCmdCompleteCallbackContext Optional completion callback context.
 *  - blockTimeMs The maximum amount of time in milliseconds to wait for the
 *    command to be posted to the MQTT agent, should the agent's event queue
 *    be full. Tasks wait in the Blocked state so don't use any CPU time.
 *
 * @note The context passed to the callback through pCmdContext member of
 * @p pCommandInfo parameter MUST remain in scope at least until the callback
 * has been executed by the agent task.
 *
 * @return #MQTTSuccess if the command was posted to the MQTT agent's event queue.
 * Otherwise an enumerated error code.
 *
 * <b>Example</b>
 * @code{c}
 *
 * // Variables used in this example.
 * MQTTAgentContext_t agentContext;
 * MQTTStatus_t status;
 * MQTTAgentCommandInfo_t commandInfo = { 0 };
 * MQTTSubscribeInfo_t subscribeInfo = { 0 };
 * MQTTAgentSubscribeArgs_t subscribeArgs = { 0 };
 *
 * // Function for command complete callback.
 * void subscribeCmdCompleteCb( MQTTAgentCommandContext_t * pCmdCallbackContext,
 *                              MQTTAgentReturnInfo_t * pReturnInfo );
 *
 * // Fill the command information.
 * commandInfo.CmdCompleteCallback = subscribeCmdCompleteCb;
 * commandInfo.blockTimeMs = 500;
 *
 * // Fill the information for topic filters to subscribe to.
 * subscribeInfo.qos = Qos1;
 * subscribeInfo.pTopicFilter = "/foo/bar";
 * subscribeInfo.topicFilterLength = strlen("/foo/bar");
 * subscribeArgs.pSubscribeInfo = &subscribeInfo;
 * subscribeArgs.numSubscriptions = 1U;
 *
 * status = MQTTAgent_Subscribe( &agentContext, &subscribeArgs, &commandInfo );
 *
 * if( status == MQTTSuccess )
 * {
 *   // Command to send subscribe request to the server has been queued. Notification
 *   // about completion of the subscribe operation will be notified to application
 *   // through invocation of subscribeCmdCompleteCb().
 * }
 *
 * @endcode
 */
/* @[declare_mqtt_agent_subscribe] */
MQTTStatus_t MQTTAgent_Subscribe( MQTTContext_t * pContext,
                                  const MQTTSubscribeInfo_t * pSubscription,
                                  uint32_t timeoutMs,
                                  QueueHandle_t uxQueue,
                                  void * Node );
/* @[declare_mqtt_agent_subscribe] */

/**
 * @brief Add a command to call MQTT_Unsubscribe() for an MQTT connection.
 *
 * @param[in] pMqttAgentContext The MQTT agent to use.
 * @param[in] pSubscriptionArgs List of topics to unsubscribe from.
 * @param[in] pCommandInfo The information pertaining to the command, including:
 *  - cmdCompleteCallback Optional callback to invoke when the command completes.
 *  - pCmdCompleteCallbackContext Optional completion callback context.
 *  - blockTimeMs The maximum amount of time in milliseconds to wait for the
 *    command to be posted to the MQTT agent, should the agent's event queue
 *    be full. Tasks wait in the Blocked state so don't use any CPU time.
 *
 * @note The context passed to the callback through pCmdContext member of
 * @p pCommandInfo parameter MUST remain in scope at least until the callback
 * has been executed by the agent task.
 *
 * @return #MQTTSuccess if the command was posted to the MQTT agent's event queue.
 * Otherwise an enumerated error code.
 *
 * <b>Example</b>
 * @code{c}
 *
 * // Variables used in this example.
 * MQTTAgentContext_t agentContext;
 * MQTTStatus_t status;
 * MQTTAgentCommandInfo_t commandInfo = { 0 };
 * MQTTSubscribeInfo_t unsubscribeInfo = { 0 };
 * MQTTAgentSubscribeArgs_t unsubscribeArgs = { 0 };
 *
 * // Function for command complete callback.
 * void unsubscribeCmdCompleteCb( MQTTAgentCommandContext_t * pCmdCallbackContext,
 *                                MQTTAgentReturnInfo_t * pReturnInfo );
 *
 * // Fill the command information.
 * commandInfo.cmdCompleteCallback = unsubscribeCmdCompleteCb;
 * commandInfo.blockTimeMs = 500;
 *
 * // Fill the information for topics to unsubscribe from.
 * unsubscribeInfo.pTopicFilter = "/foo/bar";
 * unsubscribeInfo.topicFilterLength = strlen("/foo/bar");
 * unsubscribeArgs.pSubscribeInfo = &unsubscribeInfo;
 * unsubscribeArgs.numSubscriptions = 1U;
 *
 * status = MQTTAgent_Unsubscribe( &agentContext, &unsubscribeArgs, &commandInfo );
 *
 * if( status == MQTTSuccess )
 * {
 *   // Command to send Unsubscribe request to the server has been queued. Notification
 *   // about completion of the Unsubscribe operation will be notified to application
 *   // through invocation of unsubscribeCompleteCb().
 * }
 *
 * @endcode
 */
/* @[declare_mqtt_agent_unsubscribe] */
MQTTStatus_t MQTTAgent_Unsubscribe( void );
/* @[declare_mqtt_agent_unsubscribe] */

/**
 * @brief Add a command to call MQTT_Publish() for an MQTT connection.
 *
 * @param[in] pMqttAgentContext The MQTT agent to use.
 * @param[in] pPublishInfo MQTT PUBLISH information.
 * @param[in] pCommandInfo The information pertaining to the command, including:
 *  - cmdCompleteCallback Optional callback to invoke when the command completes.
 *  - pCmdCompleteCallbackContext Optional completion callback context.
 *  - blockTimeMs The maximum amount of time in milliseconds to wait for the
 *    command to be posted to the MQTT agent, should the agent's event queue
 *    be full. Tasks wait in the Blocked state so don't use any CPU time.
 *
 * @note The context passed to the callback through pCmdContext member of
 * @p pCommandInfo parameter MUST remain in scope at least until the callback
 * has been executed by the agent task.
 *
 * @return #MQTTSuccess if the command was posted to the MQTT agent's event queue.
 * Otherwise an enumerated error code.
 *
 * <b>Example</b>
 * @code{c}
 *
 * // Variables used in this example.
 * MQTTAgentContext_t agentContext;
 * MQTTStatus_t status;
 * MQTTAgentCommandInfo_t commandInfo = { 0 };
 * MQTTPublishInfo_t publishInfo = { 0 };
 *
 * // Function for command complete callback.
 * void publishCmdCompleteCb( MQTTAgentCommandContext_t * pCmdCallbackContext,
 *                            MQTTAgentReturnInfo_t * pReturnInfo );
 *
 * // Fill the command information.
 * commandInfo.cmdCompleteCallback = publishCmdCompleteCb;
 * commandInfo.blockTimeMs = 500;
 *
 * // Fill the information for publish operation.
 * publishInfo.qos = MQTTQoS1;
 * publishInfo.pTopicName = "/some/topic/name";
 * publishInfo.topicNameLength = strlen( publishInfo.pTopicName );
 * publishInfo.pPayload = "Hello World!";
 * publishInfo.payloadLength = strlen( "Hello World!" );
 *
 * status = MQTTAgent_Publish( &agentContext, &publishInfo, &commandInfo );
 *
 * if( status == MQTTSuccess )
 * {
 *    // Command to publish message to broker has been queued.
 *    // The event of publish operation completion will be notified with
 *    // the invocation of the publishCmdCompleteCb().
 * }
 *
 * @endcode
 */
/* @[declare_mqtt_agent_publish] */
MQTTStatus_t MQTTAgent_Publish( MQTTContext_t * pContext,
                                const MQTTPublishInfo_t * pPublishInfo,
                                uint32_t timeoutMs );
/* @[declare_mqtt_agent_publish] */

/**
 * @brief Send a message to the MQTT agent purely to trigger an iteration of its loop,
 * which will result in a call to MQTT_ProcessLoop().  This function can be used to
 * wake the MQTT agent task when it is known data may be available on the connected
 * socket.
 *
 * @param[in] pMqttAgentContext The MQTT agent to use.
 * @param[in] pCommandInfo The information pertaining to the command, including:
 *  - cmdCompleteCallback Optional callback to invoke when the command completes.
 *  - pCmdCompleteCallbackContext Optional completion callback context.
 *  - blockTimeMs The maximum amount of time in milliseconds to wait for the
 *    command to be posted to the MQTT agent, should the agent's event queue
 *    be full. Tasks wait in the Blocked state so don't use any CPU time.
 *
 * @return #MQTTSuccess if the command was posted to the MQTT agent's event queue.
 * Otherwise an enumerated error code.
 *
 * <b>Example</b>
 * @code{c}
 *
 * // Variables used in this example.
 * MQTTAgentContext_t agentContext;
 * MQTTStatus_t status;
 * MQTTAgentCommandInfo_t commandInfo = { 0 };
 *
 * // Function for command complete callback.
 * void cmdCompleteCb( MQTTAgentCommandContext_t * pCmdCallbackContext,
 *                     MQTTAgentReturnInfo_t * pReturnInfo );
 *
 * // Fill the command information.
 * commandInfo.cmdCompleteCallback = cmdCompleteCb;
 * commandInfo.blockTimeMs = 500;
 *
 * status = MQTTAgent_ProcessLoop( &agentContext, &commandInfo );
 *
 * if( status == MQTTSuccess )
 * {
 *    // Command to call MQTT_ProcessLoop() has been queued.
 *    // After processing the command, if an incoming publish is received,
 *    // the event will be notified with invocation of the incoming publish
 *    // callback configured in the agent context.
 * }
 *
 * @endcode
 */
/* @[declare_mqtt_agent_processloop] */
MQTTStatus_t MQTTAgent_ProcessLoop( MQTTContext_t * pContext,
                                    uint32_t timeoutMs );
/* @[declare_mqtt_agent_processloop] */

/**
 * @brief Add a command to call MQTT_Ping() for an MQTT connection.
 *
 * @note This API function ONLY enqueues a command to send a ping request to the server,
 * and DOES NOT wait for a ping response to be received from the server. To detect whether
 * a Ping Response, has not been received from the server, the @ref MQTTAgent_CommandLoop
 * function SHOULD be used, which returns the #MQTTKeepAliveTimeout return code on a ping
 * response (or keep-alive) timeout.
 *
 * @param[in] pMqttAgentContext The MQTT agent to use.
 * @param[in] pCommandInfo The information pertaining to the command, including:
 *  - cmdCompleteCallback Optional callback to invoke when the command completes.
 *  - pCmdCompleteCallbackContext Optional completion callback context.
 *  - blockTimeMs The maximum amount of time in milliseconds to wait for the
 *    command to be posted to the MQTT agent, should the agent's event queue
 *    be full. Tasks wait in the Blocked state so don't use any CPU time.
 *
 * @note The context passed to the callback through pCmdContext member of
 * @p pCommandInfo parameter MUST remain in scope at least until the callback
 * has been executed by the agent task.
 *
 * @return #MQTTSuccess if the command was posted to the MQTT agent's event queue.
 * Otherwise an enumerated error code.
 *
 * <b>Example</b>
 * @code{c}
 *
 * // Variables used in this example.
 * MQTTAgentContext_t agentContext;
 * MQTTStatus_t status;
 * MQTTAgentCommandInfo_t commandInfo = { 0 };
 *
 * // Function for command complete callback.
 * void pingRequestCompleteCb( MQTTAgentCommandContext_t * pCmdCallbackContext,
 *                             MQTTAgentReturnInfo_t * pReturnInfo );
 *
 * // Fill the command information.
 * commandInfo.cmdCompleteCallback = pingRequestCompleteCb;
 * commandInfo.blockTimeMs = 500;
 *
 * status = MQTTAgent_Ping( &agentContext, &commandInfo );
 *
 * if( status == MQTTSuccess )
 * {
 *   // Command for sending request has been queued. Application can
 *   // handle keep-alive timeout if detected through return value of
 *   // MQTTAgent_CommandLoop in the task running the agent.
 * }
 *
 * @endcode
 */
/* @[declare_mqtt_agent_ping] */
MQTTStatus_t MQTTAgent_Ping( void );
/* @[declare_mqtt_agent_ping] */

/**
 * @brief Add a command to call MQTT_Connect() for an MQTT connection. If a session
 * is resumed with the broker, it will also resend the necessary QoS1/2 publishes.
 *
 * @note The MQTTAgent_Connect function is provided to give a thread safe equivalent
 * to the MQTT_Connect API. However, it is RECOMMENDED that instead of the application
 * tasks (i.e. tasks other than the agent task), the agent be responsible for creating
 * the MQTT connection (by calling MQTT_Connect) before starting the command loop (with
 * the MQTTAgent_CommandLoop() call). In that case, the agent SHOULD also be responsible
 * for disconnecting the MQTT connection after the command loop has terminated (through
 * an MQTTAgent_Terminate() call from an application task).
 *
 * @param[in] pMqttAgentContext The MQTT agent to use.
 * @param[in, out] pConnectArgs Struct holding args for MQTT_Connect(). On a successful
 * connection after the command is processed, the sessionPresent member will be filled
 * to indicate whether the broker resumed an existing session.
 * @param[in] pCommandInfo The information pertaining to the command, including:
 *  - cmdCompleteCallback Optional callback to invoke when the command completes.
 *  - pCmdCompleteCallbackContext Optional completion callback context.
 *  - blockTimeMs The maximum amount of time in milliseconds to wait for the
 *    command to be posted to the MQTT agent, should the agent's event queue
 *    be full. Tasks wait in the Blocked state so don't use any CPU time.
 *
 * @note The context passed to the callback through pCmdContext member of
 * @p pCommandInfo parameter MUST remain in scope at least until the callback
 * has been executed by the agent task.
 *
 * @return #MQTTSuccess if the command was posted to the MQTT agent's event queue.
 * Otherwise an enumerated error code.
 *
 * <b>Example</b>
 * @code{c}
 *
 * // Variables used in this example.
 * MQTTAgentContext_t agentContext;
 * MQTTStatus_t status;
 * MQTTConnectInfo_t connectInfo = { 0 };
 * MQTTPublishInfo_t willInfo = { 0 };
 * MQTTAgentConnectArgs_t connectionArgs;
 * MQTTAgentCommandInfo_t commandInfo = { 0 };
 *
 * // True for creating a new session with broker, false if we want to resume an old one.
 * connectInfo.cleanSession = true;
 * // Client ID must be unique to broker. This field is required.
 * connectInfo.pClientIdentifier = "someClientID";
 * connectInfo.clientIdentifierLength = strlen( connectInfo.pClientIdentifier );
 *
 * // Value for keep alive.
 * connectInfo.keepAliveSeconds = 60;
 * // The following fields are optional.
 * // Optional username and password.
 * connectInfo.pUserName = "someUserName";
 * connectInfo.userNameLength = strlen( connectInfo.pUserName );
 * connectInfo.pPassword = "somePassword";
 * connectInfo.passwordLength = strlen( connectInfo.pPassword );
 *
 * // The last will and testament is optional, it will be published by the broker
 * // should this client disconnect without sending a DISCONNECT packet.
 * willInfo.qos = MQTTQoS0;
 * willInfo.pTopicName = "/lwt/topic/name";
 * willInfo.topicNameLength = strlen( willInfo.pTopicName );
 * willInfo.pPayload = "LWT Message";
 * willInfo.payloadLength = strlen( "LWT Message" );
 *
 * // Fill the MQTTAgentConnectArgs_t structure.
 * connectArgs.pConnectInfo = &connectInfo;
 * connectArgs.pWillInfo = &willInfo;
 * // Time to block for CONNACK response when command is processed.
 * connectArgs.timeoutMs = 500;
 *
 * // Function for command complete callback.
 * void connectCmdCallback( MQTTAgentCommandContext_t * pCmdCallbackContext,
 *                          MQTTAgentReturnInfo_t * pReturnInfo );
 *
 * // Fill the command information.
 * commandInfo.cmdCompleteCallback = connectCmdCallback;
 * commandInfo.blockTimeMs = 500;
 *
 * status = MQTTAgent_Connect( &agentContext, &connectArgs, &commandInfo );
 *
 * if( status == MQTTSuccess )
 * {
 *   // Command for creating the MQTT connection has been queued.
 *   // The MQTT connection event will be notified through the
 *   // invocation of connectCmdCallback().
 * }
 * @endcode
 */
/* @[declare_mqtt_agent_connect] */
MQTTStatus_t MQTTAgent_Connect( MQTTContext_t * pContext,
                                const MQTTConnectInfo_t * pConnectInfo,
                                const MQTTPublishInfo_t * pWillInfo,
                                uint32_t timeoutMs,
                                bool * pSessionPresent );
/* @[declare_mqtt_agent_connect] */

/**
 * @brief Add a command to disconnect an MQTT connection.
 *
 * @note #MQTTAgent_CommandLoop will return after processing a #DISCONNECT
 * command to allow the network connection to be disconnected. However, any
 * pending commands in the queue, as well as those waiting for an acknowledgment,
 * will NOT be terminated.
 *
 * @note The MQTTAgent_Disconnect function is provided to give a thread safe
 * equivalent to the MQTT_Disconnect API. However, if the agent task is responsible
 * for creating the MQTT connection (before calling MQTTAgent_CommandLoop()), then
 * it is RECOMMENDED that an application task (i.e. a task other than the agent task)
 * use MQTTAgent_Terminate to terminate the command loop in the agent, and the agent
 * task be responsible for disconnecting the MQTT connection.
 *
 * @param[in] pMqttAgentContext The MQTT agent to use.
 * @param[in] pCommandInfo The information pertaining to the command, including:
 *  - cmdCompleteCallback Optional callback to invoke when the command completes.
 *  - pCmdCompleteCallbackContext Optional completion callback context.
 *  - blockTimeMs The maximum amount of time in milliseconds to wait for the
 *    command to be posted to the MQTT agent, should the agent's event queue
 *    be full. Tasks wait in the Blocked state so don't use any CPU time.
 *
 * @note The context passed to the callback through pCmdContext member of
 * @p pCommandInfo parameter MUST remain in scope at least until the callback
 * has been executed by the agent task.
 *
 * @return #MQTTSuccess if the command was posted to the MQTT agent's event queue.
 * Otherwise an enumerated error code.
 *
 * <b>Example</b>
 * @code{c}
 *
 * // Variables used in this example.
 * MQTTAgentContext_t agentContext;
 * MQTTStatus_t status;
 * MQTTAgentCommandInfo_t commandInfo = { 0 };
 *
 * // Function for command complete callback.
 * void disconnectCmdCallback( MQTTAgentCommandContext_t * pCmdCallbackContext,
 *                             MQTTAgentReturnInfo_t * pReturnInfo );
 *
 * // Fill the command information.
 * commandInfo.cmdCompleteCallback = disconnectCmdCallback;
 * commandInfo.blockTimeMs = 500;
 *
 * status = MQTTAgent_Disconnect( &agentContext, &commandInfo );
 *
 * if( status == MQTTSuccess )
 * {
 *   // Command for closing the MQTT connection has been queued.
 *   // The MQTT disconnection event will be notified through the
 *   // invocation of disconnectCmdCallback().
 * }
 *
 * @endcode
 */
/* @[declare_mqtt_agent_disconnect] */
MQTTStatus_t MQTTAgent_Disconnect( void );
/* @[declare_mqtt_agent_disconnect] */

/**
 * @brief Add a termination command to the command queue.
 *
 * On command loop termination, all pending commands in the queue, as well
 * as those waiting for an acknowledgment, will be terminated with error code
 * #MQTTRecvFailed.
 *
 * @note Commands may still be posted to the command queue after #MQTTAgent_CommandLoop
 * has returned. It is the responsibility of the application to cancel any
 * commands that are posted while the command loop is not running, such as by
 * invoking #MQTTAgent_CancelAll.
 *
 * @note We RECOMMEND that this function is used from application task(s),
 * that is a task not running the agent, to terminate the agent loop instead
 * of calling MQTTAgent_Disconnect, so that the logic for creating and closing
 * MQTT connection is owned by the agent task.
 *
 * @param[in] pMqttAgentContext The MQTT agent to use.
 * @param[in] pCommandInfo The information pertaining to the command, including:
 *  - cmdCompleteCallback Optional callback to invoke when the command completes.
 *  - pCmdCompleteCallbackContext Optional completion callback context.
 *  - blockTimeMs The maximum amount of time in milliseconds to wait for the
 *    command to be posted to the MQTT agent, should the agent's event queue
 *    be full. Tasks wait in the Blocked state so don't use any CPU time.
 *
 * @note The context passed to the callback through pCmdContext member of
 * @p pCommandInfo parameter MUST remain in scope at least until the callback
 * has been executed by the agent task.
 *
 * @return #MQTTSuccess if the command was posted to the MQTT agent's event queue.
 * Otherwise an enumerated error code.
 *
 * <b>Example</b>
 * @code{c}
 *
 * // Variables used in this example.
 * MQTTAgentContext_t agentContext;
 * MQTTStatus_t status;
 * MQTTAgentCommandInfo_t commandInfo = { 0 };
 *
 * // Function for command complete callback.
 * void terminateCallback( MQTTAgentCommandContext_t * pCmdCallbackContext,
 *                         MQTTAgentReturnInfo_t * pReturnInfo );
 *
 * // Fill the command information.
 * commandInfo.cmdCompleteCallback = terminateCallback;
 * commandInfo.blockTimeMs = 500;
 *
 * status = MQTTAgent_Terminate( &agentContext, &commandInfo );
 *
 * if( status == MQTTSuccess )
 * {
 *   // Command to terminate the agent loop has been queued.
 * }
 *
 * @endcode
 *
 */
/* @[declare_mqtt_agent_terminate] */
MQTTStatus_t MQTTAgent_Terminate( void );
/* @[declare_mqtt_agent_terminate] */

#endif /* CORE_MQTT_AGENT_H */
