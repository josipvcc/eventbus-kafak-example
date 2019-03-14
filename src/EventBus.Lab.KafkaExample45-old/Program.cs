using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventBus.Lab.KafkaExample45_old
{
    class Program
    {
        static void Main(string[] args)
        {
            string brokerList = "[NameSpace]:9093";
            string connectionString = "[ConnectionString]";
            string topic = "carinfo";
            string caCertLocation = "cacert.pem";
            string consumerGroup = "$Default";

            Console.WriteLine("Initializing Producer");
            Worker.Producer(brokerList, connectionString, topic, caCertLocation).Wait();
            Console.WriteLine();
            Console.WriteLine("Initializing Consumer");
            Worker.Consumer(brokerList, connectionString, consumerGroup, topic, caCertLocation);
            Console.ReadKey();
        }
    }
}
