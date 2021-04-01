﻿using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.ComponentModel;
using System.Text;
using System.Linq;

namespace Frends.Community.RabbitMQ
{
    /// <summary>
    /// Collection of tasks for interfacing with RabbitMQ
    /// </summary>
    public class RabbitMQTask
    {
        private static readonly ConnectionFactory Factory = new ConnectionFactory();

        private static ConcurrentDictionary<string, IBasicPublishBatch> ConcurrentDictionary = new ConcurrentDictionary<string, IBasicPublishBatch>();
        private static ConcurrentDictionary<string, IConnection> BatchChannels = new ConcurrentDictionary<string, IConnection>();

        private static IConnection CreateConnection(string hostName, bool connectWithUri)
        {
            lock (Factory)
            {
                IConnection connection;
                {
                    if (connectWithUri)
                    {
                        Factory.Uri = new Uri(hostName);
                    }
                    else
                    {
                        Factory.HostName = hostName;
                    }
                    connection = Factory.CreateConnection();
                }

                return connection;
            }
        }

        private static IConnection CreateBatchConnection(string processExecutionId, string hostName, bool connectWithUri)
        {
            lock (Factory)
            {
                return BatchChannels.GetOrAdd(processExecutionId,
                    (x) => CreateConnection(hostName, connectWithUri));
            }
        }

        private static IBasicPublishBatch CreateBasicPublishBatch(string processExecutionId, IModel channel)
        {
            return ConcurrentDictionary.GetOrAdd(processExecutionId,
                (x) => channel.CreateBasicPublishBatch());
        }

        private static void DeleteBasicPublishBatch(string processExecutionId)
        {
            ConcurrentDictionary.TryRemove(processExecutionId, out var _);
        }

        /// <summary>
        /// Closes connection and channel to RabbitMQ
        /// </summary>
        private static void CloseConnection(IModel channel, IConnection connection)
        {
            if (channel != null)
            {
                channel.Close();
                channel.Dispose();
            }
            if (connection != null)
            {
                connection.Close();
            }
        }

        /// <summary>
        /// Closes connection and channel to RabbitMQ
        /// </summary>
        private static void CloseBatchConnection(IModel channel, IConnection connection, string processExecutionId)
        {
            if (!ConcurrentDictionary.ContainsKey(processExecutionId))
            {
                if (channel != null)
                {
                    channel.Close();
                    channel.Dispose();
                }
                if (connection != null)
                {
                    connection.Close();
                    BatchChannels.TryRemove(processExecutionId, out _);
                }
            }
        }

        /// <summary>
        /// Writes messages into a queue with simple publish. Message is not under transaction. No rollback of failed messages.  
        /// </summary>
        /// <param name="inputParams"></param>
        public static bool WriteMessage([PropertyTab] WriteInputParams inputParams)
        {
            IConnection connection = CreateConnection(inputParams.HostName, inputParams.ConnectWithURI);
            IModel channel = connection.CreateModel();

            try
            {
                if (inputParams.Create)
                {
                    channel.QueueDeclare(queue: inputParams.QueueName,
                        durable: inputParams.Durable,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);
                    channel.ConfirmSelect();
                }

                IBasicProperties basicProperties = null;

                if (inputParams.Durable)
                {
                    basicProperties = channel.CreateBasicProperties();
                    basicProperties.Persistent = true;
                }


                channel.BasicPublish(exchange:
                    inputParams.ExchangeName,
                    routingKey: inputParams.RoutingKey,
                    basicProperties: basicProperties,
                    body: inputParams.Data);
                return true;
                
            }
            finally
            {
                CloseConnection(channel, connection);
            }
        }

        /// <summary>
        /// Writes messages into a queue with batch publish. All messages are under transaction. If one the message failes all messages will be rolled back.  
        /// </summary>
        /// <param name="inputParams"></param>
        public static bool WriteBatchMessage([PropertyTab] WriteBatchInputParams inputParams)
        {
            IConnection connection = CreateBatchConnection(inputParams.ProcessExecutionId, inputParams.HostName, inputParams.ConnectWithURI);
            IModel channel = connection.CreateModel();
            try
            {
                if (inputParams.Create)
                {
                    channel.QueueDeclare(queue: inputParams.QueueName,
                        durable: inputParams.Durable,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);
                    channel.TxSelect();
                }

                IBasicProperties basicProperties = null;

                if (inputParams.Durable)
                {
                    basicProperties = channel.CreateBasicProperties();
                    basicProperties.Persistent = true;
                }

                var a = channel.MessageCount(inputParams.QueueName);

                var b = ConcurrentDictionary;

                // Add message into a memory based on producer write capability.
                if (a < inputParams.WriteMessageCount)
                {
                    CreateBasicPublishBatch(inputParams.ProcessExecutionId, channel).Add(
                        exchange: inputParams.ExchangeName,
                        routingKey: inputParams.RoutingKey,
                        mandatory: true,
                        properties: basicProperties,
                        body: new ReadOnlyMemory<byte>(inputParams.Data));

                    var c = inputParams.WriteMessageCount;

                    if (ConcurrentDictionary.Count < 9)
                        return false;
                }



                try
                    {
                        CreateBasicPublishBatch(inputParams.ProcessExecutionId, channel).Publish();
                    //channel.TxCommit();
                    /*
                     rollback only when exception is thrown
                    if (channel.MessageCount(inputParams.QueueName) > 0)
                    {
                        channel.TxRollback();
                        return false;
                    }
                    */
                    //DeleteBasicPublishBatch(inputParams.ProcessExecutionId);

                    var d = channel.MessageCount(inputParams.QueueName);

                    return true;
                    }
                catch (Exception)
                {
                    channel.TxRollback();
                    return false;
                
                }
            }
            finally
            {
                CloseBatchConnection(channel, connection, inputParams.ProcessExecutionId);
            }
        }


        /// <summary>
        /// Writes message to queue. Message is a string and there is internal conversion from string to byte[] using UTF8 encoding
        /// </summary>
        /// <param name="inputParams"></param>
        /// <returns></returns>
        public static bool WriteMessageString([PropertyTab] WriteInputParamsString inputParams)
        {
            WriteInputParams wip = new WriteInputParams
            {
                ConnectWithURI = inputParams.ConnectWithURI,
                Create = inputParams.Create,
                Data = Encoding.UTF8.GetBytes(inputParams.Data),
                Durable = inputParams.Durable,
                HostName = inputParams.HostName,
                ExchangeName = inputParams.ExchangeName,
                QueueName = inputParams.QueueName,
                RoutingKey = inputParams.RoutingKey
            };

            return WriteMessage(wip);
        }

        /// <summary>
        /// Writes message to a queue in batch. Message is a string and there is internal conversion from string to byte[] using UTF8 encoding
        /// </summary>
        /// <param name="inputParams"></param>
        /// <returns></returns>
        public static bool WriteTextMessageBatch([PropertyTab] WriteBatchInputParamsString inputParams)
        {
            WriteBatchInputParams wip = new WriteBatchInputParams
            {
                ConnectWithURI = inputParams.ConnectWithURI,
                Create = inputParams.Create,
                Data = Encoding.UTF8.GetBytes(inputParams.Data),
                Durable = inputParams.Durable,
                WriteMessageCount = inputParams.WriteMessageCount,
                ProcessExecutionId = inputParams.ProcessExecutionId,
                HostName = inputParams.HostName,
                ExchangeName = inputParams.ExchangeName,
                QueueName = inputParams.QueueName,
                RoutingKey = inputParams.RoutingKey
            };

            return WriteBatchMessage(wip);
        }

        /// <summary>
        /// Reads message(s) from a queue. Returns JSON structure with message contents. Message data is byte[] encoded to base64 string
        /// </summary>
        /// <param name="inputParams"></param>
        /// <returns>JSON structure with message contents</returns>
        public static Output ReadMessage([PropertyTab] ReadInputParams inputParams)
        {
            IConnection connection = CreateConnection(inputParams.HostName, inputParams.ConnectWithURI);
            IModel channel = connection.CreateModel();
            try
            {
                Output output = new Output();

                while (inputParams.ReadMessageCount-- > 0)
                {
                    var rcvMessage = channel.BasicGet(queue: inputParams.QueueName,
                        autoAck: inputParams.AutoAck == ReadAckType.AutoAck);
                    if (rcvMessage != null)
                    {
                        output.Messages.Add(new Message
                        {
                            Data = Convert.ToBase64String(rcvMessage.Body.ToArray()),
                            MessagesCount = rcvMessage.MessageCount,
                            DeliveryTag = rcvMessage.DeliveryTag
                        });
                    }
                    //break the loop if no more messagages are present
                    else
                    {
                        break;
                    }
                }

                // Auto acking
                if (inputParams.AutoAck != ReadAckType.AutoAck && inputParams.AutoAck != ReadAckType.ManualAck)
                {
                    ManualAckType ackType = ManualAckType.NackAndRequeue;

                    switch (inputParams.AutoAck)
                    {
                        case ReadAckType.AutoNack:
                            ackType = ManualAckType.Nack;
                            break;

                        case ReadAckType.AutoNackAndRequeue:
                            ackType = ManualAckType.NackAndRequeue;
                            break;


                        case ReadAckType.AutoReject:
                            ackType = ManualAckType.Reject;
                            break;

                        case ReadAckType.AutoRejectAndRequeue:
                            ackType = ManualAckType.RejectAndRequeue;
                            break;
                    }

                    foreach (var message in output.Messages)
                    {
                        AcknowledgeMessage(channel, ackType, message.DeliveryTag);
                    }
                }

                return output;
            }
            finally
            {
                CloseConnection(channel, connection);
            }
        }

        /// <summary>
        /// Reads message(s) from a queue. Returns JSON structure with message contents. Message data is string converted from byte[] using UTF8 encoding
        /// </summary>
        /// <param name="inputParams"></param>
        /// <returns>JSON structure with message contents</returns>
        public static OutputString ReadMessageString([PropertyTab] ReadInputParams inputParams)
        {
            var messages = ReadMessage(inputParams);
            OutputString outString = new OutputString
            {
                Messages = messages.Messages.Select(m =>
                  new MessageString
                  {
                      DeliveryTag = m.DeliveryTag,
                      MessagesCount = m.MessagesCount,
                      Data = Encoding.UTF8.GetString(Convert.FromBase64String(m.Data))
                  }).ToList()
            };

            return outString;
        }

        /// <summary>
        /// Acknowledges received message. Throws exception on error.
        /// </summary>
        public static void AcknowledgeMessage(IModel channel, ManualAckType ackType, ulong deliveryTag)
        {
            if (channel == null)
            {
                // do not try to re-connect, because messages already nacked automatically
                throw new Exception("No connection to RabbitMQ");
            }

            switch (ackType)
            {
                case ManualAckType.Ack:
                    channel.BasicAck(deliveryTag, multiple: false);
                    break;

                case ManualAckType.Nack:
                    channel.BasicNack(deliveryTag, multiple: false, requeue: false);
                    break;

                case ManualAckType.NackAndRequeue:
                    channel.BasicNack(deliveryTag, multiple: false, requeue: true);
                    break;

                case ManualAckType.Reject:
                    channel.BasicReject(deliveryTag, requeue: false);
                    break;

                case ManualAckType.RejectAndRequeue:
                    channel.BasicReject(deliveryTag, requeue: true);
                    break;
                default:
                    throw new Exception("Wrong acknowledge type.");


            }
        }
    }
}
