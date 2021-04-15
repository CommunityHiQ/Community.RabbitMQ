using RabbitMQ.Client;
using System;
using System.Collections.Generic;
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
        private const string HEADER_APPID           = "X-AppId";
        private const string HEADER_CLUSTERID       = "X-ClusterId";
        private const string HEADER_CONTENTENCODING = "Content-Encoding";
        private const string HEADER_CONTENTTYPE     = "Content-Type";
        private const string HEADER_CORRELATIONID   = "X-CorrelationId";
        private const string HEADER_EXPIRATION      = "X-Expiration";
        private const string HEADER_MESSAGEID       = "X-MessageId";

        private static IConnection _connection = null;
        private static IModel _channel = null;

        private static bool IsConnectionHostNameChanged(IConnection currentConnetion, string hostName, bool connectWithURI)
        {
            // if no current connection, host name is not changed
            if (currentConnetion == null || currentConnetion.IsOpen == false)
            {
                return false;
            }

            if (connectWithURI)
            {
                var newUri = new Uri(hostName);
                return (currentConnetion.Endpoint.HostName != newUri.Host);
            }
            
            else // connect direct by host name
            {
                return (currentConnetion.Endpoint.HostName != hostName);
            }
        }

        private static void OpenConnectionIfClosed(string hostName, bool connectWithURI)
        {
            //close connection if hostname has changed
            if (IsConnectionHostNameChanged(_connection, hostName, connectWithURI)) 
            {
                CloseConnection();
            }

            if (_connection == null || _connection.IsOpen == false)
            {
                var factory = new ConnectionFactory();

                if (connectWithURI)
                {
                    factory.Uri = new Uri(hostName);
                }
                else
                {
                    factory.HostName = hostName;
                }

                _connection = factory.CreateConnection();
            }

            if (_channel == null || _channel.IsClosed)
            {
                _channel = _connection.CreateModel();
            }
        }

        /// <summary>
        /// Closes connection and channel to RabbitMQ
        /// </summary>
        public static void CloseConnection()
        {
            if (_channel != null)
            {
                _channel.Close();
            }

            if (_connection != null)
            {
                _connection.Close();
            }
        }

        /// <summary>
        /// Writes message to a queue
        /// </summary>
        /// <param name="inputParams"></param>
        public static bool WriteMessage([PropertyTab]WriteInputParams inputParams)
        {
            OpenConnectionIfClosed(inputParams.HostName, inputParams.ConnectWithURI);

            if (inputParams.Create)
            {
                _channel.QueueDeclare(queue: inputParams.QueueName,
                                durable: inputParams.Durable,
                                exclusive: false,
                                autoDelete: false,
                                arguments: null);

            }

            IBasicProperties basicProperties = _channel.CreateBasicProperties();
            basicProperties.Persistent = inputParams.Durable;
            AddHeadersToBasicProperties(basicProperties, inputParams.Headers);

            _channel.BasicPublish(exchange: inputParams.ExchangeName,
                                    routingKey: inputParams.RoutingKey,
                                    basicProperties: basicProperties,
                                    body: inputParams.Data);

            return true;
        }

        /// <summary>
        /// Writes message to queue. Message is a string and there is internal conversion from string to byte[] using UTF8 encoding
        /// </summary>
        /// <param name="inputParams"></param>
        /// <returns></returns>
        public static bool WriteMessageString([PropertyTab]WriteInputParamsString inputParams)
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
                RoutingKey = inputParams.RoutingKey,
                Headers = inputParams.Headers
            };

            return WriteMessage(wip);

        }

        /// <summary>
        /// Reads message(s) from a queue. Returns JSON structure with message contents. Message data is byte[] encoded to base64 string
        /// </summary>
        /// <param name="inputParams"></param>
        /// <returns>JSON structure with message contents: string Data, Dictionary(string,string) Headers, ulong DeliveryTag, uint MessageCount</returns>
        public static Output ReadMessage([PropertyTab]ReadInputParams inputParams)
        {
            Output output = new Output();

            OpenConnectionIfClosed(inputParams.HostName, inputParams.ConnectWithURI);

            //channel.QueueDeclare(queue: inputParams.QueueName,
            //             durable: false,
            //             exclusive: false,
            //             autoDelete: false,
            //             arguments: null);

            while (inputParams.ReadMessageCount-- > 0)
            {
                var rcvMessage = _channel.BasicGet(queue: inputParams.QueueName, autoAck: inputParams.AutoAck == ReadAckType.AutoAck);
                if (rcvMessage != null)
                {
                    output.Messages.Add(new Message
                    {
                        Data = Convert.ToBase64String(rcvMessage.Body.ToArray()),
                        Headers = GetResponseHeaderDictionary(rcvMessage.BasicProperties),
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
                    AcknowledgeMessage(ackType, message.DeliveryTag);
                }
            }

            return output;
        }

        /// <summary>
        /// Reads message(s) from a queue. Returns JSON structure with message contents. Message data is string converted from byte[] using UTF8 encoding
        /// </summary>
        /// <param name="inputParams"></param>
        /// <returns>JSON structure with message contents: string Data, Dictionary(string,string) Headers, ulong DeliveryTag, uint MessageCount</returns>
        public static OutputString ReadMessageString([PropertyTab]ReadInputParams inputParams)
        {
            var messages = ReadMessage(inputParams);
            OutputString outString = new OutputString();
            outString.Messages = messages.Messages.Select(m =>
              new MessageString
              {
                  DeliveryTag = m.DeliveryTag,
                  MessagesCount = m.MessagesCount,
                  Data = Encoding.UTF8.GetString(Convert.FromBase64String(m.Data)),
                  Headers = m.Headers
              }).ToList();

            return outString;
        }

        /// <summary>
        /// Acknowledges received message. Throws exception on error.
        /// </summary>
        /// <param name="ackType"></param>
        /// <param name="deliveryTag"></param>
        /// <returns>boolean</returns>
        public static bool AcknowledgeMessage(ManualAckType ackType, ulong deliveryTag)
        {
            if (_channel == null || _channel.IsClosed)
            {
                // do not try to re-connect, because messages already nacked automatically
                throw new Exception("No connection to RabbitMQ");
            }

            switch (ackType)
            {
                case ManualAckType.Ack:
                    _channel.BasicAck(deliveryTag, multiple: false);
                    break;

                case ManualAckType.Nack:
                    _channel.BasicNack(deliveryTag, multiple: false, requeue: false);
                    break;

                case ManualAckType.NackAndRequeue:
                    _channel.BasicNack(deliveryTag, multiple: false, requeue: true);
                    break;

                case ManualAckType.Reject:
                    _channel.BasicReject(deliveryTag, requeue: false);
                    break;

                case ManualAckType.RejectAndRequeue:
                    _channel.BasicReject(deliveryTag, requeue: true);
                    break;
            }

            return true;
        }

        private static void AddHeadersToBasicProperties(IBasicProperties basicProperties, Header[] headers)
        {
            if (headers == null) return;

            var messageHeaders = new Dictionary<string, object>();

            headers.ToList().ForEach(header => {
                switch (header.Name)
                {
                    case HEADER_APPID:
                        basicProperties.AppId = header.Value;
                        break;

                    case HEADER_CLUSTERID:
                        basicProperties.ClusterId = header.Value;
                        break;

                    case HEADER_CONTENTENCODING:
                        basicProperties.ContentEncoding = header.Value;
                        break;

                    case HEADER_CONTENTTYPE:
                        basicProperties.ContentType = header.Value;
                        break;

                    case HEADER_CORRELATIONID:
                        basicProperties.CorrelationId = header.Value;
                        break;

                    case HEADER_EXPIRATION:
                        basicProperties.Expiration = header.Value;
                        break;

                    case HEADER_MESSAGEID:
                        basicProperties.MessageId = header.Value;
                        break;

                    default:
                        messageHeaders.Add(header.Name, header.Value);
                        break;
                }
            });

            if (messageHeaders.Any())
            {
                basicProperties.Headers = messageHeaders;
            }
        }

        private static Dictionary<string, string> GetResponseHeaderDictionary(IBasicProperties basicProperties)
        {
            if (basicProperties == null) return null;

            var allHeaders = new Dictionary<string, string>()
            {
                { HEADER_APPID,             basicProperties.AppId },
                { HEADER_CLUSTERID,         basicProperties.ClusterId },
                { HEADER_CONTENTENCODING,   basicProperties.ContentEncoding },
                { HEADER_CONTENTTYPE,       basicProperties.ContentType },
                { HEADER_CORRELATIONID,     basicProperties.CorrelationId },
                { HEADER_EXPIRATION,        basicProperties.Expiration },
                { HEADER_MESSAGEID,         basicProperties.MessageId }
            }
            .Where(h => h.Value != null)
            .ToDictionary(h => h.Key, h => h.Value);

            if (basicProperties.IsHeadersPresent())
            {
                basicProperties.Headers.ToList().ForEach(x => allHeaders[x.Key] = Encoding.UTF8.GetString(x.Value as byte[]));
            }

            return allHeaders;
        }
    }
}
