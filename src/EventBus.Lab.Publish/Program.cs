using System;
using System.Configuration;
using System.Threading.Tasks;
using EventBus.Lab.Kafka;

namespace EventBus.Lab.Publish
{
    class Program
    {
        static void Main(string[] args)
        {
            Produce().Wait();
        }

        public static async Task Produce()
        {
            string brokerList = ConfigurationManager.AppSettings["eventHubsNamespaceURL"];
            string password = ConfigurationManager.AppSettings["eventHubsConnStr"];
            string topicName = ConfigurationManager.AppSettings["eventHubName"];
            string caCertLocation = ConfigurationManager.AppSettings["caCertLocation"];
            string consumerGroup = ConfigurationManager.AppSettings["consumerGroup"];

            var producer = new Producer(brokerList, password, consumerGroup, "");

            var message = $"Sending message at {DateTime.Now.ToString("yyyy-MM-dd HH:mm")}";

            await producer.ProduceAsync(topicName, message);
        }
    }
}
