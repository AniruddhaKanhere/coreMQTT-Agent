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
 * @file core_mqtt_agent.c
 * @brief Implements an MQTT agent (or daemon task) to enable multi-threaded access to
 * coreMQTT.
 *
 * @note Implements an MQTT agent (or daemon task) on top of the coreMQTT MQTT client
 * library.  The agent makes coreMQTT usage thread safe by being the only task (or
 * thread) in the system that is allowed to access the native coreMQTT API - and in
 * so doing, serializes all access to coreMQTT even when multiple tasks are using the
 * same MQTT connection.
 *
 * The agent provides an equivalent API for each coreMQTT API.  Whereas coreMQTT
 * APIs are prefixed "MQTT_", the agent APIs are prefixed "MQTTAgent_".  For example,
 * that agent's MQTTAgent_Publish() API is the thread safe equivalent to coreMQTT's
 * MQTT_Publish() API.
 */

/* Standard includes. */
#include <string.h>
#include <stdio.h>
#include <assert.h>

/* MQTT agent include. */
#include "core_mqtt_agent.h"

/*-----------------------------------------------------------*/

#define AGENT_OUTSTANDING_MSGS     10

#define agentINCOMING_ACK          0x01

#define mqttagentTOPIC_LENGTH      20
#define mqttagentMAX_TOPICS        5

#define mqttagentSTACK_SIZE        800

#define mqttagentUSE_AGENT_TASK    1

typedef struct xTaskTable
{
    TaskHandle_t xTaskHandle;
    uint16_t usPacketID;
    uint16_t usStale;
} TaskTable_t;

typedef struct xNode
{
    QueueHandle_t xQueue;
    struct xNode * pxNext;
} Node_t;

typedef struct LinkedList
{
    char pucTopic[ mqttagentTOPIC_LENGTH ];
    uint16_t usTopicNameLength;
    MQTTQoS_t xQoS;
    Node_t * Head;
} LinkedList_t;

#if ( mqttagentUSE_AGENT_TASK == 1 )
    /* Stack size of the agent task should be in words, not bytes. */
    static StackType_t mqttAgentStack[ mqttagentSTACK_SIZE ];

/* Task Control block used for Agent task. */
    static StaticTask_t xAgentTaskBuffer;

/* Handle of the Agent task. */
    static TaskHandle_t xMQTTAgentTaskHandle = NULL;

/* Linked list for all the topics and their subscribers. */
    static LinkedList_t AgentLinkedLists[ mqttagentMAX_TOPICS ] = { { { 0 } } };

/* Table to track ACKs. This is used only in case of QoS1. */
    static TaskTable_t AgentTable[ AGENT_OUTSTANDING_MSGS ];
#endif /* if ( mqttagentUSE_AGENT_TASK == 1 ) */

/* The below variable will be set to pdTRUE when the call the MQTT_Connect succeeds.
 * This triggers the MQTT Agent to run. */
static volatile BaseType_t xConnected = pdFALSE;

/* Mutex used to serialize access to the MQTT library APIs. */
static SemaphoreHandle_t MQTTAgentMutex = NULL;
/* Static memory used to create the mutex. */
static StaticSemaphore_t xMutexBuffer;

/* The below variable is set in the MQTT callback in case any incoming packet is received.
 * This signals the MQTT agent to run the MQTT_ProcessLoop function again after which this
 * variable is reset. */
static volatile BaseType_t packetReceivedInLoop;

#if ( mqttagentUSE_AGENT_TASK == 1 )

    static void prvMQTTAgentTask( void * pvParameters );

    static BaseType_t AddToTable( TaskHandle_t xTaskHandle,
                                  uint16_t usPacketID );

    static TaskHandle_t FindAndRemoveFromTable( uint16_t usPacketID );

    static void vMarkStaleEntry( uint16_t usPacketID );

    static BaseType_t HandleIncomingACKs( uint16_t packetIdentifier );

    static BaseType_t AddQueueToSubscriptionList( const MQTTSubscribeInfo_t * pSubscription,
                                                  QueueHandle_t uxQueue,
                                                  Node_t * Node );

    static BaseType_t HandleIncomingPublishes( MQTTPacketInfo_t * pPacketInfo,
                                               MQTTPublishInfo_t * pIncomingPublishInfo,
                                               uint16_t packetIdentifier );

    static void mqttEventCallback( MQTTContext_t * pMqttContext,
                                   MQTTPacketInfo_t * pPacketInfo,
                                   MQTTDeserializedInfo_t * pDeserializedInfo );
#endif /* if ( mqttagentUSE_AGENT_TASK == 1 ) */



#if ( mqttagentUSE_AGENT_TASK == 1 )

/*
 * @brief Add the packet ID and task handle to an empty entry in the table.
 *        These IDs and task handles are used to keep track of ACKs corresponding
 *        to publishes.
 *
 * @param[in] xTaskHandle Task handle of the task which is waiting for an ACK for a
 *                        sent publish.
 * @param[in] usPacketID The packet ID of the publish.
 */
    static BaseType_t AddToTable( TaskHandle_t xTaskHandle,
                                  uint16_t usPacketID )
    {
        int i;
        BaseType_t xReturn = pdFAIL;

        for( i = 0; i < AGENT_OUTSTANDING_MSGS; i++ )
        {
        	/* Cleanup any stale entries. */
        	if( AgentTable[ i ].usStale == pdTRUE )
        	{
        		memset( &AgentTable[ i ], 0x00, sizeof( TaskTable_t ) );
        	}

            if( AgentTable[ i ].xTaskHandle == NULL )
            {
                AgentTable[ i ].xTaskHandle = xTaskHandle;
                AgentTable[ i ].usPacketID = usPacketID;
                AgentTable[ i ].usStale = pdFALSE;
                break;
            }
        }

        if( i != AGENT_OUTSTANDING_MSGS )
        {
            xReturn = pdPASS;
        }

        return xReturn;
    }

/*-----------------------------------------------------------*/

/*
 * @brief Find a packet ID in the table and if found, remove it.
 *
 * @param[in] The packet ID which is to be searched for.
 *
 * @return The task handle corresponding to the packet ID. If not
 *         found, NULL is returned.
 */
    static TaskHandle_t FindAndRemoveFromTable( uint16_t usPacketID )
    {
        int i;
        TaskHandle_t xReturn = NULL;

        for( i = 0; i < AGENT_OUTSTANDING_MSGS; i++ )
        {
        	/* Cleanup any stale entries before attempting to find out a task
        	 * handle corresponding to a packet ID. */
			if( AgentTable[ i ].usStale == pdTRUE )
			{
				memset( &AgentTable[ i ], 0x00, sizeof( TaskTable_t ) );
			}

            if( AgentTable[ i ].usPacketID == usPacketID )
            {
                xReturn = AgentTable[ i ].xTaskHandle;

                AgentTable[ i ].xTaskHandle = NULL;
                AgentTable[ i ].usPacketID = 0;
                AgentTable[ i ].usStale = pdFALSE;

                break;
            }
        }

        return xReturn;
    }

/*-----------------------------------------------------------*/

/*
 * @brief Mark the entry in the ACK table as stale.
 *
 * @param[in] usPacketID packet ID corresponding to the entry to be
 *            marked stale.
 */
static void vMarkStaleEntry( uint16_t usPacketID )
{
	int i;

	for( i = 0; i < AGENT_OUTSTANDING_MSGS; i++ )
	{
		/* Find and mark the stale entry. */
		if( AgentTable[ i ].usPacketID == usPacketID )
		{
			/* The stale entry will be removed while adding or
			 * removing from the table. */
			AgentTable[ i ].usStale = pdTRUE;
			break;
		}
	}
}

/*-----------------------------------------------------------*/

/*
 * @brief Handle the incoming ACKs by finding the task handle corresponding to
 *        the packet ID and notify the task that an ACK has been received.
 *
 * @param[in] packetIdentifier The packet ID of the incoming ACK.
 *
 * @return Returns pdPASS if the corresponding task was found and notified.
 *         Otherwise, pdFAIL is returned.
 */
    static BaseType_t HandleIncomingACKs( uint16_t packetIdentifier )
    {
        TaskHandle_t xTaskToNotify;
        BaseType_t xReturn = pdFAIL;

        xTaskToNotify = FindAndRemoveFromTable( packetIdentifier );

        if( xTaskToNotify != NULL )
        {
            ( void ) xTaskNotify( xTaskToNotify,
                                  agentINCOMING_ACK,
                                  eSetValueWithOverwrite );

            xReturn = pdPASS;
        }
        else
        {
            LogError( ( "Incoming ACK for unwanted packet ID %d", packetIdentifier ) );
        }

        return xReturn;
    }

/*-----------------------------------------------------------*/

/*
 * @brief Add a queue handle to a node and add it to a linked list for the topic filter.
 *
 * @param[in] pSubscription Subscription information.
 * @param[in] uxQueue The queue handle which is to be added to the node.
 * @param[in] Node The pointer a Node data structure. This memory is owned by the
 *                 application.
 *
 * @return If the queue handle was added to a node successfully then a pdPASS is returned.
 *         Otherwise a pdFAIL is returned.
 */
    static BaseType_t AddQueueToSubscriptionList( const MQTTSubscribeInfo_t * pSubscription,
                                                  QueueHandle_t uxQueue,
                                                  Node_t * Node )
    {
        BaseType_t xReturn = pdFAIL;
        UBaseType_t i;
        Node_t * pxCurrent, * pxPrevious = NULL;
        const char * topic;
        LinkedList_t * pxEmptyLinkedList = NULL;

        for( i = 0; i < mqttagentMAX_TOPICS; i++ )
        {
            topic = AgentLinkedLists[ i ].pucTopic;

            /* If any matching topic is found with similar QoS. */
            if( ( strncmp( topic, pSubscription->pTopicFilter, mqttagentTOPIC_LENGTH ) == 0 ) && ( AgentLinkedLists[ i ].xQoS == pSubscription->qos ) )
            {
                pxCurrent = AgentLinkedLists[ i ].Head;
                pxPrevious = pxCurrent;

                /* Find the tail of the linked list */
                while( pxCurrent != NULL )
                {
                    pxPrevious = pxCurrent;
                    pxCurrent = pxCurrent->pxNext;
                }

                pxPrevious->pxNext = Node;
                pxPrevious->pxNext->xQueue = uxQueue;
                pxPrevious->pxNext->pxNext = NULL;

                xReturn = pdPASS;

                break;
            }
            else if( AgentLinkedLists[ i ].pucTopic[ 0 ] == '\0' )
            {
                pxEmptyLinkedList = &AgentLinkedLists[ i ];
            }
        }

        /* If we reached the end without finding a match. */
        if( i == mqttagentMAX_TOPICS )
        {
            if( pxEmptyLinkedList != NULL )
            {
                pxEmptyLinkedList->Head = Node;
                pxEmptyLinkedList->Head->xQueue = uxQueue;
                pxEmptyLinkedList->Head->pxNext = NULL;
                pxEmptyLinkedList->usTopicNameLength = strlen( pSubscription->pTopicFilter );

                strcpy( pxEmptyLinkedList->pucTopic, pSubscription->pTopicFilter );

                xReturn = pdPASS;
            }
        }

        return xReturn;
    }

/*-----------------------------------------------------------*/

/*
 * @brief Send the incoming publish to all the queues in the linked list.
 *
 * @param[in] xList The lined list
 * @param[in] pvPayload The incoming publish
 * @param[in] uxPayloadLength The length of the incoming publish
 *
 * @return pdPASS is always returned
 */
    static BaseType_t xSendToAllQueues( LinkedList_t xList,
                                        const void * pvPayload,
                                        size_t uxPayloadLength )
    {
        BaseType_t xReturn = pdPASS;

        Node_t * pxCurrent = xList.Head;

        while( pxCurrent != NULL )
        {
            xQueueSendToBack( pxCurrent->xQueue, pvPayload, portMAX_DELAY );

            pxCurrent = pxCurrent->pxNext;
        }

        return xReturn;
    }

/*-----------------------------------------------------------*/

/**
 * @brief Handle incoming publishes and dispatch them to various queues.
 *
 * @param[in] pPacketInfo The packet information data structure.
 * @param[in] pIncomingPublishInfo Data about incoming publish.
 * @param[in] packetIdentifier MQTT Packet ID.
 *
 * @return pdPASS is returned if the incoming publish's topic matches to
 *         one of the linked lists. pdFAIL is returned otherwise.
 */
    static BaseType_t HandleIncomingPublishes( MQTTPacketInfo_t * pPacketInfo,
                                               MQTTPublishInfo_t * pIncomingPublishInfo,
                                               uint16_t packetIdentifier )
    {
        BaseType_t xReturn = pdFAIL;
        bool IsMatching;
        UBaseType_t i;

        ( void ) pPacketInfo;

        for( i = 0; i < mqttagentMAX_TOPICS; i++ )
        {
            if( AgentLinkedLists[ i ].pucTopic[ 0 ] != '\0' )
            {
                IsMatching = false;

                MQTT_MatchTopic( pIncomingPublishInfo->pTopicName,
                                 pIncomingPublishInfo->topicNameLength,
                                 AgentLinkedLists[ i ].pucTopic,
                                 AgentLinkedLists[ i ].usTopicNameLength,
                                 &IsMatching );

                if( IsMatching == true )
                {
                    /* Found a match. Send the message to all the queues. */
                    xSendToAllQueues( AgentLinkedLists[ i ], pIncomingPublishInfo->pPayload, pIncomingPublishInfo->payloadLength );
                    xReturn = pdPASS;
                }
            }
        }

        return xReturn;
    }

/*-----------------------------------------------------------*/

/*
 * @brief The MQTT Agent task. This task is responsible for running the MQTT ProcessLoop
 *        command periodically. Ideally, this should be run only when there is data present
 *        in the socket to be read. However, the offloaded stack doesn't have one such
 *        function, thus, we have added a task delay.
 *
 * @param[in] pvParameters The MQTT context is passed in as a pointer to this task.
 */
    static void prvMQTTAgentTask( void * pvParameters )
    {
        MQTTContext_t * pContext = ( MQTTContext_t * ) pvParameters;

        do
        {
            if( xConnected == pdTRUE )
            {
                MQTTAgent_ProcessLoop( pContext, portMAX_DELAY );
            }

            /* Ideally this should be a "block on a notification" from socket signaling that
             * data is available to be read. However, the Inventek WiFi module doesn't have
             * that functionality and thus an arbitrary delay had to be inserted. */
            vTaskDelay( pdMS_TO_TICKS( 8 ) );
        } while( 1 );

        /* Delete the task if it is complete; which it never should. */
        LogInfo( ( "MQTT Agent task completed." ) );
        vTaskDelete( NULL );
    }

/*-----------------------------------------------------------*/

#endif /* if ( mqttagentUSE_AGENT_TASK == 1 ) */

/*
 * @brief Callback for any incoming data. Such as ACK or a publish.
 *
 * @param[in] pMqttContext The global MQTT context
 * @param[in] pPacketInfo Incoming packet's information
 * @param[in] pDeserializedInfo Deserialized packet information
 */
static void mqttEventCallback( MQTTContext_t * pMqttContext,
                               MQTTPacketInfo_t * pPacketInfo,
                               MQTTDeserializedInfo_t * pDeserializedInfo )
{
    uint16_t packetIdentifier = pDeserializedInfo->packetIdentifier;
    const uint8_t upperNibble = ( uint8_t ) 0xF0;

    assert( pMqttContext != NULL );
    assert( pPacketInfo != NULL );

    /* This callback executes from within MQTT_ProcessLoop().  Setting this flag
     * indicates that the callback executed so the caller of MQTT_ProcessLoop() knows
     * it should call it again as there may be more data to process. */
    packetReceivedInLoop = true;

    #if ( mqttagentUSE_AGENT_TASK == 1 )

        /* Handle incoming publish. The lower 4 bits of the publish packet type is used
         * for the dup, QoS, and retain flags. Hence masking out the lower bits to check
         * if the packet is publish. */
        if( ( pPacketInfo->type & upperNibble ) == MQTT_PACKET_TYPE_PUBLISH )
        {
            /* Handle the incoming publish. */
            ( void ) HandleIncomingPublishes( pPacketInfo, pDeserializedInfo->pPublishInfo, packetIdentifier );
        }
        else
        {
            /* Handle other packets. */
            switch( pPacketInfo->type )
            {
                case MQTT_PACKET_TYPE_PUBACK:
                case MQTT_PACKET_TYPE_PUBCOMP: /* QoS 2 command completion. */
                case MQTT_PACKET_TYPE_SUBACK:
                case MQTT_PACKET_TYPE_UNSUBACK:
                    ( void ) HandleIncomingACKs( packetIdentifier );
                    break;

                /* Nothing to do for these packets since they don't indicate command completion. */
                case MQTT_PACKET_TYPE_PUBREC:
                case MQTT_PACKET_TYPE_PUBREL:
                    break;

                /* Any other packet type is invalid. */
                case MQTT_PACKET_TYPE_PINGRESP:
                default:
                    LogError( ( "Unknown packet type received:(%02x).\n",
                                pPacketInfo->type ) );
                    break;
            }
        }
    #endif /* if ( mqttagentUSE_AGENT_TASK == 1 ) */
}

/*-----------------------------------------------------------*/

/*
 * @brief Initialize the MQTT Agent
 *
 * @param[in] pContext A pointer to the global MQTT context
 * @param[in] pTransportInterface The transport interface
 * @param[in] getTimeFunction The function used to get current time
 * @param[in] pNetworkBuffer The network buffer
 * @param[in] uxMQTTAgentPriority The priority of the MQTT Agent task
 *
 * @return MQTTSuccess is returned in case the Agent is initialized
 *         successfully. Otherwise an error is return.
 */
MQTTStatus_t MQTTAgent_Init( MQTTContext_t * pContext,
                             const TransportInterface_t * pTransportInterface,
                             MQTTGetCurrentTimeFunc_t getTimeFunction,
                             const MQTTFixedBuffer_t * pNetworkBuffer,
                             UBaseType_t uxMQTTAgentPriority )
{
    MQTTStatus_t returnStatus;

    if( ( pContext == NULL ) ||
        ( pTransportInterface == NULL ) ||
        ( getTimeFunction == NULL ) )
    {
        returnStatus = MQTTBadParameter;
    }
    else
    {
        ( void ) memset( pContext, 0x00, sizeof( MQTTContext_t ) );

        #if ( mqttagentUSE_AGENT_TASK == 1 )
            ( void ) memset( AgentTable, 0x00, AGENT_OUTSTANDING_MSGS * sizeof( TaskTable_t ) );
        #endif

        returnStatus = MQTT_Init( pContext,
                                  pTransportInterface,
                                  getTimeFunction,
                                  mqttEventCallback,
                                  pNetworkBuffer );

        if( returnStatus == MQTTSuccess )
        {
            MQTTAgentMutex = xSemaphoreCreateMutexStatic( &xMutexBuffer );
            configASSERT( MQTTAgentMutex );

            if( MQTTAgentMutex == NULL )
            {
                returnStatus = MQTTNoMemory;
            }
            else
            {
                #if ( mqttagentUSE_AGENT_TASK == 1 )

                    /* Create an instance of the MQTT agent task. Give it higher priority than the
                     * subscribe-publish tasks so that the agent's command queue will not become full,
                     * as those tasks need to send commands to the queue. */
                    xMQTTAgentTaskHandle = xTaskCreateStatic( prvMQTTAgentTask,
                                                              "MQTT-Agent",
                                                              mqttagentSTACK_SIZE,
                                                              ( void * ) pContext,
                                                              uxMQTTAgentPriority,
                                                              mqttAgentStack,
                                                              &xAgentTaskBuffer );

                    if( xMQTTAgentTaskHandle == NULL )
                    {
                        returnStatus = MQTTNoMemory;
                    }
                #endif /* if ( mqttagentUSE_AGENT_TASK == 1 ) */
            }
        }
    }

    return returnStatus;
}

/*-----------------------------------------------------------*/

/*
 * @brief Subscribe to a given topic filter.
 *
 * @param[in] pContext A pointer to the global MQTT context
 * @param[in] pSubscription Subscription info for one topic filter
 * @param[in] timeoutMs Maximum time in milliseconds that this function can
 *                      block for. Provide portMAX_DELAY if you'd like this
 *                      function to block forever.
 * @param[in] uxQueue The handle to the queue to which the incoming publishes will
 *                    be posted. Caller should create a queue and pass in the handle.
 * @param[in] pNode A node which will be consumed by the agent to maintain a linked
 *                  list. Caller should provide this memory to the agent.
 *
 * @return MQTTSuccess is returned in case the Agent is initialized
 *         successfully. Otherwise an error is return.
 */
MQTTStatus_t MQTTAgent_Subscribe( MQTTContext_t * pContext,
                                  const MQTTSubscribeInfo_t * pSubscription,
								  TickType_t timeoutMs,
                                  QueueHandle_t uxQueue,
                                  void * pNode )
{
    MQTTStatus_t statusReturn = MQTTBadParameter;
    uint16_t usPacketId;
    uint32_t ulReturn = pdFALSE;
    TimeOut_t xTimeOut;
    TickType_t xTicksToWait = pdMS_TO_TICKS( timeoutMs );

    configASSERT( uxQueue != NULL );
    configASSERT( pNode != NULL );

    /* Initialize xTimeOut. This records the time at which this function was
     * entered. */
    vTaskSetTimeOutState( &xTimeOut );


    #if ( mqttagentUSE_AGENT_TASK == 1 )
        if( xSemaphoreTake( MQTTAgentMutex, xTicksToWait ) == pdPASS )
        {
        	usPacketId = MQTT_GetPacketId( pContext );
            statusReturn = MQTT_Subscribe( pContext,
                                           pSubscription,
                                           1,
										   usPacketId );

            if( statusReturn == MQTTSuccess )
            {
                AddQueueToSubscriptionList( pSubscription,
                                            uxQueue,
                                            ( Node_t * ) pNode );

                AddToTable( xTaskGetCurrentTaskHandle(), usPacketId );

                xSemaphoreGive( MQTTAgentMutex );

                /* Check for timeout. */
                if( xTaskCheckForTimeOut( &xTimeOut, &xTicksToWait ) == pdFALSE )
                {
                    ulReturn = ulTaskNotifyTake( pdTRUE,
                	    	                     xTicksToWait );
                }

                if( ulReturn == pdFALSE )
				{
					LogWarn( ( "Timeout while waiting for a SUBACK" ) );

					/* Mark the ACK entry as stale. */
					vMarkStaleEntry( usPacketId );

					statusReturn = MQTTKeepAliveTimeout;
				}
            }
        }
        else
        {
            /* Returning this value to depict timeout since MQTT does
             * not have a better method right now. */
            statusReturn = MQTTKeepAliveTimeout;
        }
    #endif /* if ( mqttagentUSE_AGENT_TASK == 1 ) */

    return statusReturn;
}

/*-----------------------------------------------------------*/

/*
 * @brief Publish to a given MQTT Agent topic
 *
 * @param[in] pContext A pointer to the global MQTT context
 * @param[in] pPublishInfo Publish information
 * @param[in] timeoutMs Maximum amount of time this function blocks.
 *                      portMAX_DELAY should be passed if the call can be
 *                      allowed to block forever.
 *
 * @return MQTTSuccess is returned in case the Agent is initialized
 *         successfully. Otherwise an error is return.
 */
MQTTStatus_t MQTTAgent_Publish( MQTTContext_t * pContext,
                                const MQTTPublishInfo_t * pPublishInfo,
								const MQTTAgentCommandInfo_t * pCommandInfo )
{
    MQTTStatus_t statusReturn = MQTTKeepAliveTimeout;
    uint16_t usPacketID;
    uint32_t ulReturn;
    TimeOut_t xTimeOut;
    TickType_t xTicksToWait = pdMS_TO_TICKS( pCommandInfo->blockTimeMs );
    BaseType_t xIsTimeOut, xSuccessWithQoS1;

    /* Initialize xTimeOut. This records the time at which this function was
     * entered. */
    vTaskSetTimeOutState( &xTimeOut );

    if( xSemaphoreTake( MQTTAgentMutex, xTicksToWait ) == pdPASS )
    {
        if( pPublishInfo->qos != MQTTQoS0 )
        {
            usPacketID = MQTT_GetPacketId( pContext );
        }

        statusReturn = MQTT_Publish( pContext, pPublishInfo, usPacketID );

        /* Add the entry to table in case of a successful QoS1 publish. */
        xSuccessWithQoS1 = ( statusReturn == MQTTSuccess ) && ( pPublishInfo->qos != MQTTQoS0 );
        if( xSuccessWithQoS1 != pdFALSE )
		{
        	/* Adding to the table must happen before releasing semaphore. */
			AddToTable( xTaskGetCurrentTaskHandle(), usPacketID );
		}

        /* Return the semaphore. Following operations can happen without holding
         * the semaphore. */
        ( void ) xSemaphoreGive( MQTTAgentMutex );

        /* If the QoS1 publish was sent successfully, then wait for the ACK. */
        if( xSuccessWithQoS1 != pdFALSE )
		{
			xIsTimeOut = xTaskCheckForTimeOut( &xTimeOut, &xTicksToWait );

			/* Check for timeout. */
			if( xIsTimeOut == pdFALSE )
			{
				ulReturn = ulTaskNotifyTake( pdTRUE,
											 xTicksToWait );
			}

			if( ( xIsTimeOut == pdTRUE ) || ( ulReturn == pdFALSE ) )
			{
				LogWarn( ( "Timeout while waiting for a PUBACK" ) );

				/* Mark the ACK entry as stale. */
				vMarkStaleEntry( usPacketID );

				statusReturn = MQTTKeepAliveTimeout;
			}
		}
    }

    return statusReturn;
}

/*-----------------------------------------------------------*/

/*
 * @brief Call the MQTT Process loop till data is available in socket.
 *
 * @param[in] pContext A pointer to the global MQTT context
 * @param[in] timeoutMs Maximum time this function can block.
 *
 * @return Return the value received from the MQTT_ProcessLoop function.
 */
MQTTStatus_t MQTTAgent_ProcessLoop( MQTTContext_t * pContext,
		                            TickType_t timeoutMs )
{
    MQTTStatus_t statusReturn = MQTTBadParameter;
    TimeOut_t xTimeOut;
    TickType_t xTicksToWait = pdMS_TO_TICKS( timeoutMs );

    /* Initialize xTimeOut. This records the time at which this function was
     * entered. */
    vTaskSetTimeOutState( &xTimeOut );

    if( xSemaphoreTake( MQTTAgentMutex, xTicksToWait ) == pdPASS )
    {
        do
        {
            packetReceivedInLoop = false;

            statusReturn = MQTT_ProcessLoop( pContext,
                                             0 );
            if( xTaskCheckForTimeOut( &xTimeOut, &xTicksToWait ) == pdTRUE )
            {
            	break;
            }
        } while( packetReceivedInLoop == true );

        xSemaphoreGive( MQTTAgentMutex );
    }

    return statusReturn;
}

/*-----------------------------------------------------------*/

/*
 * @brief Connect to the MQTT broker
 *
 * @param[in] pContext A pointer to the global MQTT context
 * @param[in] pConnectInfo Information for connection
 * @param[in] pWillInfo The last will and testament information
 * @param[in] timeoutMs Maximum time this function can block. However this
 *                      is not used currently.
 * @param[in] pSessionPresent Is the session present
 *
 * @return Return the value received from the MQTT_Connect function.
 */
MQTTStatus_t MQTTAgent_Connect( MQTTContext_t * pContext,
                                const MQTTConnectInfo_t * pConnectInfo,
                                const MQTTPublishInfo_t * pWillInfo,
								TickType_t timeoutMs,
                                bool * pSessionPresent )
{
    MQTTStatus_t statusReturn = MQTTKeepAliveTimeout;
    uint32_t LocaltimeoutMs = timeoutMs;

    if( xSemaphoreTake( MQTTAgentMutex, LocaltimeoutMs ) == pdPASS )
    {
        statusReturn = MQTT_Connect( pContext, pConnectInfo, pWillInfo, timeoutMs, pSessionPresent );

        if( statusReturn == MQTTSuccess )
        {
            /* Mark that the MQTT session is connected. */
            xConnected = pdTRUE;
        }

        xSemaphoreGive( MQTTAgentMutex );
    }

    return statusReturn;
}

/*-----------------------------------------------------------*/

/*
 * @brief Unsubscribe from a given topic
 */
MQTTStatus_t MQTTAgent_Unsubscribe( void )
{
    MQTTStatus_t statusReturn = MQTTBadParameter;

    /* Implementation TBD. */

    return statusReturn;
}

/*-----------------------------------------------------------*/

/*
 * @brief Disconnect from the MQTT broker
 */
MQTTStatus_t MQTTAgent_Disconnect( void )
{
    MQTTStatus_t statusReturn = MQTTBadParameter;

    /* Implementation TBD. */

    /*
     * - set xConnected to false
     */

    return statusReturn;
}

/*-----------------------------------------------------------*/

/*
 * @brief Ping the MQTT agent
 */
MQTTStatus_t MQTTAgent_Ping( void )
{
    MQTTStatus_t statusReturn = MQTTBadParameter;

    /* Implementation TBD. */

    return statusReturn;
}

/*-----------------------------------------------------------*/

/*
 * @brief Terminate the MQTT Agent
 */
MQTTStatus_t MQTTAgent_Terminate( void )
{
    MQTTStatus_t statusReturn = MQTTBadParameter;

    /* Implementation TBD. */

    /*
     * Delete the agent task.
     */

    return statusReturn;
}

/*-----------------------------------------------------------*/
