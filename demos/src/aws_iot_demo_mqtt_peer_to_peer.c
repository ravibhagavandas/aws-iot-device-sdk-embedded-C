/*
 * Copyright (C) 2018 Amazon.com, Inc. or its affiliates.  All Rights Reserved.
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
 * @file aws_iot_demo_mqtt_peer_to_peer.c
 * @brief Demonstrates how the MQTT library can be used to receive peer to peer messages
 * and unsolicited messages from broker without subscribing to the topic at the broker.
 */

/* The config header is always included first. */
#include "iot_config.h"

/* Standard includes. */
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Set up logging for this demo. */
#include "iot_demo_logging.h"

/* Platform layer includes. */
#include "platform/iot_clock.h"
#include "platform/iot_threads.h"

/* MQTT include. */
#include "iot_mqtt.h"

/**
 * @brief A topic subscribed by the device to receive message from the broker.
 */
#define IOT_DEMO_MQTT_DEVICE_TOPIC                  "iotdemo/device"

/**
 * @brief A topic used to publish the received messages by the device.
 */
#define IOT_DEMO_MQTT_DEVICE_MSG_RECIEVED_TOPIC        "iotdemo/device/messages/received"

/**
 * @brief Topic to which the device publishes all the unsolicited messages it received. 
 */
#define IOT_DEMO_MQTT_UNSOLICITED_MSG_TOPIC            "iotdemo/device/messages/unsolicited"

/**
 * @brief Wildcard topic to receive all the messages.
 *
 */
#define IOT_DEMO_MQTT_WILDCARD_TOPIC                   "#"


#define IOT_DEMO_MAX_PUBLISH_COUNT                     ( 1000 )

#define IOT_DEMO_PUBLISH_INTERVAL_MS                   ( 2000 )

#define TOPIC_NAME_LENGTH( topicName )        ( ( uint16_t ) ( sizeof( topicName ) - 1 ) )

/**
 * @brief The first characters in the client identifier. A timestamp is appended
 * to this prefix to create a unique client identifer.
 
 */
#define CLIENT_IDENTIFIER                 "TEST_DEVICE"


/**
 * @brief The keep-alive interval used for this demo.
 *
 * An MQTT ping request will be sent periodically at this interval.
 */
#define KEEP_ALIVE_SECONDS                       ( 60 )

/**
 * @brief The timeout for MQTT operations in this demo.
 */
#define MQTT_TIMEOUT_MS                          ( 5000 )

/**
 * @brief Format for the payload string which the device publishes to the broker.
 */
#define PUBLISH_PAYLOAD_FORMAT                   "Hello world %d!"

/**
 * @brief Size of the buffer that holds the PUBLISH messages in this demo.
 */
#define PUBLISH_PAYLOAD_BUFFER_LENGTH            ( sizeof( PUBLISH_PAYLOAD_FORMAT ) + 2 )

/**
 * @brief The maximum number of times each PUBLISH in this demo will be retried.
 */
#define PUBLISH_RETRY_LIMIT                      ( 10 )

/**
 * @brief A PUBLISH message is retried if no response is received within this
 * time.
 */
#define PUBLISH_RETRY_MS                         ( 1000 )


/*-----------------------------------------------------------*/

/* Declaration of demo function. */
int RunMqttPeerToPeerDemo( bool awsIotMqttMode,
                 const char * pIdentifier,
                 void * pNetworkServerInfo,
                 void * pNetworkCredentialInfo,
                 const IotNetworkInterface_t * pNetworkInterface );

/*-----------------------------------------------------------*/

/**
 * @brief Callback is intended to receive and process all unsolicited messages for which 
 * there is no MQTT broker subscription.
 * 
 * @param[in] param1 Not used.
 * @param[in] pPublish Information about the incoming PUBLISH message passed by
 * the MQTT library.
 * 
 */
static void _unsolicitedMessagesCallback( void * param1,
                                          IotMqttCallbackParam_t * const pPublish )
{

    const char * pPayload = pPublish->u.message.info.pPayload;
    IotMqttPublishInfo_t publishInfo = IOT_MQTT_PUBLISH_INFO_INITIALIZER;
    const char *pTopic = pPublish->u.message.info.pTopicName;
    size_t topicLength = pPublish->u.message.info.topicNameLength;

    (void) param1;

    if( strncmp( pTopic, IOT_DEMO_MQTT_DEVICE_TOPIC, topicLength ) != 0 )
    {
        /* Print information about the unsolicited PUBLISH message. */
        IotLogInfo( "Unsolicited message received:\r\n"
                    "Subscription topic filter: %.*s\r\n"
                    "Publish topic name: %.*s\r\n"
                    "Publish retain flag: %d\r\n"
                    "Publish QoS: %d\r\n"
                    "Publish payload: %.*s",
                    pPublish->u.message.topicFilterLength,
                    pPublish->u.message.pTopicFilter,
                    pPublish->u.message.info.topicNameLength,
                    pPublish->u.message.info.pTopicName,
                    pPublish->u.message.info.retain,
                    pPublish->u.message.info.qos,
                    pPublish->u.message.info.payloadLength,
                    pPayload );

        /* Republish the message back to a different topic. */
        publishInfo.qos = IOT_MQTT_QOS_1;
        publishInfo.pTopicName = IOT_DEMO_MQTT_UNSOLICITED_MSG_TOPIC;
        publishInfo.topicNameLength = TOPIC_NAME_LENGTH(IOT_DEMO_MQTT_UNSOLICITED_MSG_TOPIC);
        publishInfo.pPayload = pPayload;
        publishInfo.payloadLength = pPublish->u.message.info.payloadLength;
        publishInfo.retryMs = PUBLISH_RETRY_MS;
        publishInfo.retryLimit = PUBLISH_RETRY_LIMIT;

        if( IotMqtt_PublishSync( pPublish->mqttConnection,
                                &publishInfo,
                                0,
                                MQTT_TIMEOUT_MS ) == IOT_MQTT_SUCCESS )
        {
            IotLogInfo( "Message republished to topic: %.*s",
                        publishInfo.topicNameLength,
                        publishInfo.pTopicName );
        }
        else
        {
            IotLogInfo( "Failed to republish message to topic: %.*s",
                        publishInfo.topicNameLength,
                        publishInfo.pTopicName );
        }
    }
                                              
}

/*-----------------------------------------------------------*/

/**
 * @brief Called by the MQTT library when an incoming PUBLISH message is received.
 *
 * The demo uses this callback to handle incoming PUBLISH messages and republishes
 * the messages on a different topic.
 * 
 * @param[in] param1 Not used.
 * @param[in] pPublish Information about the incoming PUBLISH message passed by
 * the MQTT library.
 */
static void _incomingMessagesCallback( void * param1,
                                       IotMqttCallbackParam_t * const pPublish )
{
    const char * pPayload = pPublish->u.message.info.pPayload;
    IotMqttPublishInfo_t publishInfo = IOT_MQTT_PUBLISH_INFO_INITIALIZER;

    (void) param1;

    /* Print information about the incoming PUBLISH message. */
    IotLogInfo( "Incoming message received:\r\n"
                "Subscription topic filter: %.*s\r\n"
                "Publish topic name: %.*s\r\n"
                "Publish retain flag: %d\r\n"
                "Publish QoS: %d\r\n"
                "Publish payload: %.*s",
                pPublish->u.message.topicFilterLength,
                pPublish->u.message.pTopicFilter,
                pPublish->u.message.info.topicNameLength,
                pPublish->u.message.info.pTopicName,
                pPublish->u.message.info.retain,
                pPublish->u.message.info.qos,
                pPublish->u.message.info.payloadLength,
                pPayload );

    /* Republish the message back to a different topic. */
    publishInfo.qos = IOT_MQTT_QOS_1;
    publishInfo.pTopicName = IOT_DEMO_MQTT_DEVICE_MSG_RECIEVED_TOPIC;
    publishInfo.topicNameLength = TOPIC_NAME_LENGTH(IOT_DEMO_MQTT_DEVICE_MSG_RECIEVED_TOPIC);
    publishInfo.pPayload = pPayload;
    publishInfo.payloadLength = pPublish->u.message.info.payloadLength;
    publishInfo.retryMs = PUBLISH_RETRY_MS;
    publishInfo.retryLimit = PUBLISH_RETRY_LIMIT;

    if( IotMqtt_PublishSync( pPublish->mqttConnection,
                             &publishInfo,
                             0,
                             MQTT_TIMEOUT_MS ) == IOT_MQTT_SUCCESS )
    {
        IotLogInfo( "Message republished to topic: %.*s",
                    publishInfo.topicNameLength,
                    publishInfo.pTopicName );
    }
    else
    {
        IotLogInfo( "Failed to republish message to topic: %.*s",
                    publishInfo.topicNameLength,
                    publishInfo.pTopicName );
    }
}

/*-----------------------------------------------------------*/

/**
 * @brief Initialize the MQTT library.
 *
 * @return `EXIT_SUCCESS` if all libraries were successfully initialized;
 * `EXIT_FAILURE` otherwise.
 */
static int _initializeDemo( void )
{
    int status = EXIT_SUCCESS;
    IotMqttError_t mqttInitStatus = IOT_MQTT_SUCCESS;

    mqttInitStatus = IotMqtt_Init();

    if( mqttInitStatus != IOT_MQTT_SUCCESS )
    {
        /* Failed to initialize MQTT library. */
        status = EXIT_FAILURE;
    }

    return status;
}

/*-----------------------------------------------------------*/

/**
 * @brief Clean up the MQTT library.
 */
static void _cleanupDemo( void )
{
    IotMqtt_Cleanup();
}

/*-----------------------------------------------------------*/

/**
 * @brief Establish a new connection to the MQTT server.
 *
 * @param[in] awsIotMqttMode Specify if this demo is running with the AWS IoT
 * MQTT server. Set this to `false` if using another MQTT server.
 * @param[in] pIdentifier NULL-terminated MQTT client identifier.
 * @param[in] pNetworkServerInfo Passed to the MQTT connect function when
 * establishing the MQTT connection.
 * @param[in] pNetworkCredentialInfo Passed to the MQTT connect function when
 * establishing the MQTT connection.
 * @param[in] pNetworkInterface The network interface to use for this demo.
 * @param[out] pMqttConnection Set to the handle to the new MQTT connection.
 *
 * @return `EXIT_SUCCESS` if the connection is successfully established; `EXIT_FAILURE`
 * otherwise.
 */
static int _establishMqttConnection( bool awsIotMqttMode,
                                     const char * pIdentifier,
                                     void * pNetworkServerInfo,
                                     void * pNetworkCredentialInfo,
                                     const IotNetworkInterface_t * pNetworkInterface,
                                     IotMqttConnection_t * pMqttConnection )
{
    int status = EXIT_SUCCESS;
    IotMqttError_t connectStatus = IOT_MQTT_STATUS_PENDING;
    IotMqttNetworkInfo_t networkInfo = IOT_MQTT_NETWORK_INFO_INITIALIZER;
    IotMqttConnectInfo_t connectInfo = IOT_MQTT_CONNECT_INFO_INITIALIZER;
    IotMqttSubscription_t wildCardSubscription = IOT_MQTT_SUBSCRIPTION_INITIALIZER;

    /* Set the members of the network info not set by the initializer. This
     * struct provided information on the transport layer to the MQTT connection. */
    networkInfo.createNetworkConnection = true;
    networkInfo.u.setup.pNetworkServerInfo = pNetworkServerInfo;
    networkInfo.u.setup.pNetworkCredentialInfo = pNetworkCredentialInfo;
    networkInfo.pNetworkInterface = pNetworkInterface;

    #if ( IOT_MQTT_ENABLE_SERIALIZER_OVERRIDES == 1 ) && defined( IOT_DEMO_MQTT_SERIALIZER )
        networkInfo.pSerializer = IOT_DEMO_MQTT_SERIALIZER;
    #endif

    /* Set the members of the connection info not set by the initializer. */
    connectInfo.awsIotMqttMode = awsIotMqttMode;
    connectInfo.cleanSession = true;
    connectInfo.keepAliveSeconds = KEEP_ALIVE_SECONDS;
    connectInfo.pWillInfo = NULL;

    wildCardSubscription.pTopicFilter = IOT_DEMO_MQTT_WILDCARD_TOPIC;
    wildCardSubscription.topicFilterLength = TOPIC_NAME_LENGTH(IOT_DEMO_MQTT_WILDCARD_TOPIC);
    wildCardSubscription.qos = 0;
    wildCardSubscription.callback.function = _unsolicitedMessagesCallback;
    wildCardSubscription.callback.pCallbackContext = NULL;

    connectInfo.pPreviousSubscriptions = &wildCardSubscription;
    connectInfo.previousSubscriptionCount = 1;

    connectInfo.pClientIdentifier = CLIENT_IDENTIFIER;
    connectInfo.clientIdentifierLength = ( uint16_t ) strlen( connectInfo.pClientIdentifier );

    /* Establish the MQTT connection. */
    if( status == EXIT_SUCCESS )
    {
        IotLogInfo( "MQTT demo client identifier is %.*s (length %hu).",
                    connectInfo.clientIdentifierLength,
                    connectInfo.pClientIdentifier,
                    connectInfo.clientIdentifierLength );

        connectStatus = IotMqtt_Connect( &networkInfo,
                                         &connectInfo,
                                         MQTT_TIMEOUT_MS,
                                         pMqttConnection );

        if( connectStatus != IOT_MQTT_SUCCESS )
        {
            IotLogError( "MQTT CONNECT returned error %s.",
                         IotMqtt_strerror( connectStatus ) );

            status = EXIT_FAILURE;
        }
    }

    return status;
}

/*-----------------------------------------------------------*/

/**
 * @brief Add or remove subscriptions by either subscribing or unsubscribing.
 *
 * @param[in] mqttConnection The MQTT connection to use for subscriptions.
 * @param[in] operation Either #IOT_MQTT_SUBSCRIBE or #IOT_MQTT_UNSUBSCRIBE.
 * @param[in] pTopicFilters Array of topic filters for subscriptions.
 * @param[in] pCallbackParameter The parameter to pass to the subscription
 * callback.
 *
 * @return `EXIT_SUCCESS` if the subscription operation succeeded; `EXIT_FAILURE`
 * otherwise.
 */
static int _modifySubscription( IotMqttConnection_t mqttConnection,
                                 IotMqttOperationType_t operation,
                                 const char * pTopicFilter,
                                 void * pCallbackParameter )
{
    int status = EXIT_SUCCESS;
    int32_t i = 0;
    IotMqttError_t subscriptionStatus = IOT_MQTT_STATUS_PENDING;
    IotMqttSubscription_t subscription = IOT_MQTT_SUBSCRIPTION_INITIALIZER;

    subscription.qos = IOT_MQTT_QOS_1;
    subscription.pTopicFilter = pTopicFilter;
    subscription.topicFilterLength = strlen(pTopicFilter);
    subscription.callback.pCallbackContext = pCallbackParameter;
    subscription.callback.function = _incomingMessagesCallback;

    /* Modify subscriptions by either subscribing or unsubscribing. */
    if( operation == IOT_MQTT_SUBSCRIBE )
    {
        subscriptionStatus = IotMqtt_SubscribeSync( mqttConnection,
                                                    &subscription,
                                                    1,
                                                    0,
                                                    MQTT_TIMEOUT_MS );

        /* Check the status of SUBSCRIBE. */
        switch( subscriptionStatus )
        {
            case IOT_MQTT_SUCCESS:
                IotLogInfo( "Demo topic filter subscriptions accepted." );
                break;

            case IOT_MQTT_SERVER_REFUSED:

            /* Check which subscriptions were rejected before exiting the demo. */
                if( IotMqtt_IsSubscribed( mqttConnection,
                                            subscription.pTopicFilter,
                                            subscription.topicFilterLength,
                                            NULL ) == true )
                {
                    IotLogInfo( "Topic filter %.*s was accepted.",
                                subscription.topicFilterLength,
                                subscription.pTopicFilter );
                }
                else
                {
                    IotLogError( "Topic filter %.*s was rejected.",
                                    subscription.topicFilterLength,
                                    subscription.pTopicFilter );
                }

                status = EXIT_FAILURE;
                break;

            default:

                status = EXIT_FAILURE;
                break;
        }
    }
    else if( operation == IOT_MQTT_UNSUBSCRIBE )
    {
        subscriptionStatus = IotMqtt_UnsubscribeSync( mqttConnection,
                                                      &subscription,
                                                      1,
                                                      0,
                                                      MQTT_TIMEOUT_MS );

        /* Check the status of UNSUBSCRIBE. */
        if( subscriptionStatus != IOT_MQTT_SUCCESS )
        {
            status = EXIT_FAILURE;
        }
    }
    else
    {
        /* Only SUBSCRIBE and UNSUBSCRIBE are valid for modifying subscriptions. */
        IotLogError( "MQTT operation %s is not valid for modifying subscriptions.",
                     IotMqtt_OperationType( operation ) );

        status = EXIT_FAILURE;
    }

    return status;
}

/*-----------------------------------------------------------*/

/**
 * @brief Keep publishing to the  topic at regular intervals.
 *
 * @param[in] mqttConnection The MQTT connection to use for publishing.
 * @param[in] pTopic Topic name used for publishing.
 * @param[in] pPublishReceivedCounter Counts the number of messages received on
 * topic filters.
 *
 * @return `EXIT_SUCCESS` if all messages are published and received; `EXIT_FAILURE`
 * otherwise.
 */
static int _publishMessages( IotMqttConnection_t mqttConnection, const char * pTopic )
{
    int status = EXIT_SUCCESS;
    intptr_t publishCount = 0, i = 0;
    IotMqttError_t publishStatus = IOT_MQTT_STATUS_PENDING;
    IotMqttPublishInfo_t publishInfo = IOT_MQTT_PUBLISH_INFO_INITIALIZER;
    char pPublishPayload[ PUBLISH_PAYLOAD_BUFFER_LENGTH ] = { 0 };

    /* Set the common members of the publish info. */
    publishInfo.qos = IOT_MQTT_QOS_1;
    publishInfo.pTopicName = pTopic;
    publishInfo.topicNameLength = strlen(pTopic);
    publishInfo.pPayload = pPublishPayload;
    publishInfo.retryMs = PUBLISH_RETRY_MS;
    publishInfo.retryLimit = PUBLISH_RETRY_LIMIT;

    /* Loop to PUBLISH all messages of this demo. */
    for( publishCount = 0; publishCount < IOT_DEMO_MAX_PUBLISH_COUNT;  publishCount++ )
    {

        IotLogInfo( "Publishing messages %d", publishCount );
        /* Pass the PUBLISH number to the operation complete callback. */
                
        /* Generate the payload for the PUBLISH. */
        status = snprintf( pPublishPayload,
                        PUBLISH_PAYLOAD_BUFFER_LENGTH,
                        PUBLISH_PAYLOAD_FORMAT,
                        ( int ) publishCount );

        /* Check for errors from snprintf. */
        if( status < 0 )
        {
            IotLogError( "Failed to generate MQTT PUBLISH payload for PUBLISH %d.",
                        ( int ) publishCount );
            status = EXIT_FAILURE;

            break;
        }
        else
        {
            publishInfo.payloadLength = ( size_t ) status;
            status = EXIT_SUCCESS;
        }

        /* PUBLISH a message. This is an asynchronous function that notifies of
        * completion through a callback. */
        publishStatus = IotMqtt_PublishSync( mqttConnection,
                                    &publishInfo,
                                    0,
                                    MQTT_TIMEOUT_MS );
        if( publishStatus != IOT_MQTT_SUCCESS )
        {
            IotLogError( "MQTT PUBLISH %d returned error %s.",
                        ( int ) publishCount,
                        IotMqtt_strerror( publishStatus ) );
            status = EXIT_FAILURE;

            break;
        }

        IotClock_SleepMs( IOT_DEMO_PUBLISH_INTERVAL_MS );
    }

    return status;
}

/*-----------------------------------------------------------*/

/**
 * @brief The function that runs the MQTT peer to peer demo, called by the demo runner.
 *
 * @param[in] awsIotMqttMode Specify if this demo is running with the AWS IoT
 * MQTT server. Set this to `false` if using another MQTT server.
 * @param[in] pIdentifier NULL-terminated MQTT client identifier.
 * @param[in] pNetworkServerInfo Passed to the MQTT connect function when
 * establishing the MQTT connection.
 * @param[in] pNetworkCredentialInfo Passed to the MQTT connect function when
 * establishing the MQTT connection.
 * @param[in] pNetworkInterface The network interface to use for this demo.
 *
 * @return `EXIT_SUCCESS` if the demo completes successfully; `EXIT_FAILURE` otherwise.
 */
int RunMqttPeerToPeerDemo( bool awsIotMqttMode,
                 const char * pIdentifier,
                 void * pNetworkServerInfo,
                 void * pNetworkCredentialInfo,
                 const IotNetworkInterface_t * pNetworkInterface )
{
    /* Return value of this function and the exit status of this program. */
    int status = EXIT_SUCCESS;

    /* Handle of the MQTT connection used in this demo. */
    IotMqttConnection_t mqttConnection = IOT_MQTT_CONNECTION_INITIALIZER;


    /* Flags for tracking which cleanup functions must be called. */
    bool librariesInitialized = false, connectionEstablished = false;

    /* Initialize the libraries required for this demo. */
    status = _initializeDemo();

    if( status == EXIT_SUCCESS )
    {
        /* Mark the libraries as initialized. */
        librariesInitialized = true;

        /* Establish a new MQTT connection. */
        status = _establishMqttConnection( awsIotMqttMode,
                                           pIdentifier,
                                           pNetworkServerInfo,
                                           pNetworkCredentialInfo,
                                           pNetworkInterface,
                                           &mqttConnection );
    }

    if( status == EXIT_SUCCESS )
    {
        /* Mark the MQTT connection as established. */
        connectionEstablished = true;

        /* Add the topic filter subscriptions used in this demo. */
        status = _modifySubscription( mqttConnection,
                                       IOT_MQTT_SUBSCRIBE,
                                       IOT_DEMO_MQTT_DEVICE_TOPIC,
                                       NULL );
    }

    if( status == EXIT_SUCCESS )
    {
        
        _publishMessages( mqttConnection,
                          IOT_DEMO_MQTT_DEVICE_TOPIC );
    }

    if( status == EXIT_SUCCESS )
    {
        /* Remove the topic subscription filters used in this demo. */
        status = _modifySubscription( mqttConnection,
                                       IOT_MQTT_UNSUBSCRIBE,
                                       IOT_DEMO_MQTT_DEVICE_TOPIC,
                                       NULL );
    }

    /* Disconnect the MQTT connection if it was established. */
    if( connectionEstablished == true )
    {
        IotMqtt_Disconnect( mqttConnection, 0 );
    }

    /* Clean up libraries if they were initialized. */
    if( librariesInitialized == true )
    {
        _cleanupDemo();
    }

    return status;
}

/*-----------------------------------------------------------*/
