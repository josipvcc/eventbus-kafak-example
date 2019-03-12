using System;
using System.Configuration;
using EventBus.Lab.Kafka;

namespace EventBus.Lab.KafkaExample
{
    class Program
    {
        static void Main(string[] args)
        {
            var brokerList = ConfigurationManager.AppSettings["eventHubsNamespaceURL"];
            var password = ConfigurationManager.AppSettings["eventHubsConnStr"];
            var topicName = ConfigurationManager.AppSettings["eventHubName"];
            var consumerGroup = ConfigurationManager.AppSettings["consumerGroup"];

            var worker = new Worker(brokerList, password, consumerGroup);
            
            Console.WriteLine("Initialize Producer");
            worker.ProduceAsync(topicName, "hello").Wait();
            
            Console.WriteLine("Initialize Consumer");
            worker.Consumer(topicName);
            
            Console.ReadLine();
        }
    }
}