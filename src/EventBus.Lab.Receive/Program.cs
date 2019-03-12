using System;
using System.Configuration;
using System.Threading.Tasks;
using EventBus.Lab.Kafka;

namespace EventBus.Lab.Receive
{
    class Program
    {
        static void Main(string[] args)
        {
            Consume();
        }

        public static void Consume()
        {
            string brokerList = ConfigurationManager.AppSettings["eventHubsNamespaceURL"];
            string password = ConfigurationManager.AppSettings["eventHubsConnStr"];
            string topicName = ConfigurationManager.AppSettings["eventHubName"];
            string caCertLocation = ConfigurationManager.AppSettings["caCertLocation"];
            string consumerGroup = ConfigurationManager.AppSettings["consumerGroup"];

            var producer = new Producer(brokerList, password, consumerGroup, "");

            producer.Consume(topicName);
        }
    }
}
