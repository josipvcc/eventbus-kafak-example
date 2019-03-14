using Confluent.Kafka;
using Confluent.Kafka.Serialization;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.Lab.KafkaExample45_old
{
    public static class Worker
    {
        public static async Task Producer(string brokerList, string connStr, string topic, string cacertlocation)
        {
            try
            {
                var config = new Dictionary<string, object> {
                    { "bootstrap.servers", brokerList },
                    { "security.protocol", "SASL_SSL" },
                    { "sasl.mechanism", "PLAIN" },
                    { "sasl.username", "$ConnectionString" },
                    { "sasl.password", connStr },
                    { "ssl.ca.location", cacertlocation },
                    //{ "debug", "security,broker,protocol" }       //Uncomment for librdkafka debugging information
                };

                using (var producer = new Producer<long, string>(config, new LongSerializer(), new StringSerializer(Encoding.UTF8)))
                {
                    Console.WriteLine("Sending 2 messages to topic: " + topic + ", broker(s): " + brokerList);
                    for (int x = 0; x < 2; x++)
                    {
                        var msg = string.Format("Sample message #{0} sent at {1}", x, DateTime.Now.ToString("yyyy-MM-dd_HH:mm:ss.ffff"));
                        var deliveryReport = await producer.ProduceAsync(topic, DateTime.UtcNow.Ticks, msg);
                        Console.WriteLine(string.Format("Message {0} sent (value: '{1}')", x, msg));
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(string.Format("Exception Occurred - {0}", e.Message));
            }
        }

        public static void Consumer(string brokerList, string connStr, string consumergroup, string topic, string cacertlocation)
        {
            var config = new Dictionary<string, object> {
                    { "bootstrap.servers", brokerList },
                    { "security.protocol","SASL_SSL" },
                    { "sasl.mechanism","PLAIN" },
                    { "sasl.username", "$ConnectionString" },
                    { "sasl.password", connStr },
                    { "ssl.ca.location", cacertlocation },
                    { "group.id", consumergroup },
                    { "request.timeout.ms", 60000 },
                    { "broker.version.fallback", "1.0.0" },         //Event Hubs for Kafka Ecosystems supports Kafka v1.0+, 
                                                                    //a fallback to an older API will fail
                    //{ "debug", "security,broker,protocol" }       //Uncomment for librdkafka debugging information
                };

            using (var consumer = new Consumer<long, string>(config, new LongDeserializer(), new StringDeserializer(Encoding.UTF8)))
            {
                consumer.OnMessage += (_, msg)
                  => Console.WriteLine($"Received: '{msg.Value}'");

                consumer.OnError += (_, error)
                  => Console.WriteLine($"Error: {error}");

                consumer.OnConsumeError += (_, msg)
                  => Console.WriteLine($"Consume error ({msg.TopicPartitionOffset}): {msg.Error}");

                Console.WriteLine("Consuming messages from topic: " + topic + ", broker(s): " + brokerList);
                consumer.Subscribe(topic);

                while (true)
                {
                    consumer.Poll(TimeSpan.FromMilliseconds(1000));
                }
            }

        }
    }
}
