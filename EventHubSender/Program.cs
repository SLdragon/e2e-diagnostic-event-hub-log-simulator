using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;

namespace EventHubSender
{
    class ByteArrayToHexStringConverter
    {
        static readonly uint[] lookup32 = CreateLookup32();
        static uint[] CreateLookup32()
        {
            var result = new uint[256];
            for (int i = 0; i < 256; i++)
            {
                string s = i.ToString("X2");
                result[i] = ((uint)s[0]) + ((uint)s[1] << 16);
            }
            return result;
        }
        internal static string Convert(byte[] bytes)
        {
            var result = new char[bytes.Length * 2];
            for (int i = 0; i < bytes.Length; i++)
            {
                var val = lookup32[bytes[i]];
                result[2 * i] = (char)val;
                result[2 * i + 1] = (char)(val >> 16);
            }
            return new string(result);
        }
    }

    class Record
    {
        public string time = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");
        public string resourceId = "/SUBSCRIPTIONS/FAAB228D-DF7A-4086-991E-E81C4659D41A/RESOURCEGROUPS/RENTU-E2E-DEV/PROVIDERS/MICROSOFT.DEVICES/IOTHUBS/RENTU-E2E-DEV-IOTHUB";
        public string operationName;
        public string durationMs;
        public string correlationId;
        public string properties;
        public string level = "Information";
    }

    class EventHubMessage
    {
        public List<Record> records = new List<Record>();
    }

    class Program
    {
        static readonly string connectionString = Environment.GetEnvironmentVariable("E2E_DIAGNOSTICS_EVENT_HUB_CONNECTION_STRING", EnvironmentVariableTarget.User);
        static readonly string eventHubName = "insights-logs-e2ediagnostics";
        static string[] endpointNames = new string[] { "myEndpoint1", "myEndpoint2", "myEndpoint3" };
        static string[] deviceNames = new string[] { "myDevice1", "myDevice2", "myDevice3" };
        static Random random = new Random();

        static void Main(string[] args)
        {
            Console.WriteLine("Press Ctrl-C to stop the sender process");
            SendingRandomMessages();
        }

        static Record GenerateD2CLogs(string spanId, string deviceName)
        {
            var record = new Record();
            record.operationName = "DiagnosticIoTHubD2C";
            record.durationMs = "";
            record.correlationId = $"00-8cd869a412459a25f5b4f31311223344-{spanId}-01";

            var callerDateTime = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");
            var calleeDateTime = (DateTime.Now.AddMilliseconds(random.Next(500, 1000))).ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");
            
            record.properties = $"{{\"messageSize\":\"1000\",\"deviceId\":\"{deviceName}\",\"callerLocalTimeUtc\":\"{callerDateTime}\",\"calleeLocalTimeUtc\":\"{calleeDateTime}\"}}";

            if(random.Next(1000) == 100)
            {
                record.level = "Error";
            }

            return record;
        }

        static Record GenerateIngressLogs(string parentSpanId)
        {
            var record = new Record();
            record.operationName = "DiagnosticIoTHubIngress";
            record.durationMs = random.Next(1, 1000).ToString();
            record.correlationId = "00-8cd869a412459a25f5b4f31311223344-0144d2590aacd909-01";
            record.properties = $"{{\"isRoutingEnabled\":\"true\",\"parentSpanId\":\"{parentSpanId}\"}}";
            if (random.Next(1000) == 100)
            {
                record.level = "Error";
            }
            return record;
        }

        static Record GenerateEgressLogs(string endpointName)
        {
            var record = new Record();
            record.operationName = "DiagnosticIoTHubEgress";
            record.durationMs = random.Next(1, 1000).ToString();
            record.correlationId = "00-8cd869a412459a25f5b4f31311223344-0144d2590aacd909-01";
            record.properties = $"{{\"endpointType\":\"EventHub\",\"endpointName\":\"{endpointName}\",\"parentSpanId\":\"349810a9bbd28730\"}}";
            if (random.Next(1000) == 100)
            {
                record.level = "Error";
            }
            return record;
        }

        static void SendRandomLogs(EventHubClient eventHubClient)
        {
            var rand = random.Next(1, 3);

            var eventhubMessage = new EventHubMessage();
            string message = "";
            if (rand == 1)
            {
                var bytes = new byte[8];
                random.NextBytes(bytes);
                var spanId = ByteArrayToHexStringConverter.Convert(bytes);
                var randDeviceName = deviceNames[random.Next(deviceNames.Length)];
                var d2cLog = GenerateD2CLogs(spanId, randDeviceName);
                var ingressLog = GenerateIngressLogs(spanId);

                eventhubMessage.records.Add(d2cLog);
                eventhubMessage.records.Add(ingressLog);

                message = JsonConvert.SerializeObject(eventhubMessage);
            }
            else if (rand == 2)
            {
                var randPointName = endpointNames[random.Next(endpointNames.Length)];
                var egressLog = GenerateEgressLogs(randPointName);
                eventhubMessage.records.Add(egressLog);
                message = JsonConvert.SerializeObject(eventhubMessage);
            }

            eventHubClient.Send(new EventData(Encoding.UTF8.GetBytes(message)));
        }

        static void SendingRandomMessages()
        {
            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);
            while (true)
            {
                try
                {
                    var message = Guid.NewGuid().ToString();
                    Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, message);
                    SendRandomLogs(eventHubClient);
                }
                catch (Exception exception)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("{0} > Exception: {1}", DateTime.Now, exception.Message);
                    Console.ResetColor();
                }

                Thread.Sleep(2000);
            }
        }
    }
}
