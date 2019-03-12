using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace EventBus.Lab.Kafka
{
    public class Worker
    {
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        public Worker(string brokerList, string connStr, string groupId)
        {
            _producerConfig = new ProducerConfig {
                BootstrapServers = brokerList,
                SaslUsername = "$ConnectionString",
                SaslPassword = connStr,
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                Debug = "security,broker,protocol",
                MessageTimeoutMs = 5000,
                MessageSendMaxRetries = 0
            };

            _consumerConfig = new ConsumerConfig{
                BootstrapServers = brokerList,
                SaslUsername = "$ConnectionString",
                SaslPassword = connStr,
                SaslMechanism = SaslMechanism.Plain,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                GroupId = groupId,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                Debug = "security,broker,protocol",
                AutoCommitIntervalMs = 5000
            };
        }

        public async Task ProduceAsync(string topic, string message)
        {
            using (var p = new ProducerBuilder<Null, string>(_producerConfig).Build())
            {
                try
                {
                    var dr = await p.ProduceAsync(topic, new Message<Null, string> { Value=message });
                    Console.WriteLine($"Delivered '{dr.Value}' to '{dr.TopicPartitionOffset}'");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                }
            }
        }

        public void Consumer(string topic)
        {
            using (var c = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build())
            {
                c.Subscribe(topic);

                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) => {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = c.Consume(cts.Token);
                            Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Error occured: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}
