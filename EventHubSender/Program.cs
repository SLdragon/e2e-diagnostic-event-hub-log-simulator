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
        static string[] serviceNames = new string[] { "thirdPartyService1", "thirdPartyService2", "thirdPartyService3" };
        static Random random = new Random();

        const bool SendThirdPartyLogs = false;

        static string GenerateTraceId(string prefix, string spanId = null)
        {
            if (spanId == null)
            {
                var bytes = new byte[8];
                random.NextBytes(bytes);
                spanId = ByteArrayToHexStringConverter.Convert(bytes);
            }

            return $"00-{prefix}-{spanId}-01";
        }

        static void Main(string[] args)
        {
            Console.WriteLine("Press Ctrl-C to stop the sender process");
            SendingRandomMessages();
        }

        static Record GenerateD2CLogs(string correlationId, string deviceName)
        {
            var record = new Record();
            record.operationName = "DiagnosticIoTHubD2C";
            record.durationMs = "";
            record.correlationId = correlationId;
            record.time = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

            var callerDateTime = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");
            var calleeDateTime = (DateTime.Now.AddMilliseconds(random.Next(500, 1000))).ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

            record.properties = $"{{\"messageSize\":\"1000\",\"deviceId\":\"{deviceName}\",\"callerLocalTimeUtc\":\"{callerDateTime}\",\"calleeLocalTimeUtc\":\"{calleeDateTime}\"}}";

            if (random.Next(1000) == 100)
            {
                record.level = "Error";
            }

            return record;
        }

        static Record GenerateIngressLogs(string correlationId, string parentSpanId)
        {
            var record = new Record();
            record.operationName = "DiagnosticIoTHubIngress";
            record.durationMs = random.Next(1, 1000).ToString();
            record.correlationId = correlationId;
            record.time = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

            record.properties = $"{{\"isRoutingEnabled\":\"true\",\"parentSpanId\":\"{parentSpanId}\"}}";
            if (random.Next(1000) == 100)
            {
                record.level = "Error";
            }
            return record;
        }

        static Record GenerateEgressLogs(string correlationId, string parentSpanId, string endpointName)
        {
            var record = new Record();
            record.operationName = "DiagnosticIoTHubEgress";
            record.durationMs = random.Next(1, 1000).ToString();
            record.correlationId = correlationId;
            record.time = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

            record.properties = $"{{\"endpointType\":\"EventHub\",\"endpointName\":\"{endpointName}\",\"parentSpanId\":\"{parentSpanId}\"}}";
            if (random.Next(1000) == 100)
            {
                record.level = "Error";
            }
            return record;
        }

        static Record GenerateThirdPartyD2CLogs(string correlationId, string serviceName, string endpointName)
        {
            var record = new Record();
            record.operationName = "ThirdPartyServiceD2CLog";
            record.durationMs = random.Next(1, 1000).ToString();
            record.correlationId = correlationId;
            record.time = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

            var callerDateTime = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");
            var calleeDateTime = (DateTime.Now.AddMilliseconds(random.Next(500, 1000))).ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

            record.properties = $"{{\"messageSize\":\"1000\", \"endpointName\":\"{endpointName}\",\"thirdPartyServiceName\":\"{serviceName}\",\"callerLocalTimeUtc\":\"{callerDateTime}\",\"calleeLocalTimeUtc\":\"{calleeDateTime}\"}}";

            if (random.Next(1000) == 100)
            {
                record.level = "Error";
            }

            return record;
        }

        static Record GenerateThirdPartyIngressLogs(string correlationId, string parentSpanId, string serviceName)
        {
            var record = new Record();
            record.operationName = "ThirdPartyServiceIngressLog";
            record.durationMs = random.Next(1, 1000).ToString();
            record.correlationId = correlationId;
            record.time = DateTime.Now.ToUniversalTime().ToString("yyyy'-'MM'-'dd'T'HH':'mm':'ss'.'fff'Z'");

            record.properties = $"{{\"isRoutingEnabled\":\"true\",\"thirdPartyServiceName\":\"{serviceName}\",\"parentSpanId\":\"{parentSpanId}\"}}";
            if (random.Next(1000) == 100)
            {
                record.level = "Error";
            }
            return record;
        }

        static void SendRandomLogs(EventHubClient eventHubClient)
        {
            var eventhubMessage = new EventHubMessage();
            string message = "";

            var prefixBytes = new byte[16];
            random.NextBytes(prefixBytes);
            string prefix = ByteArrayToHexStringConverter.Convert(prefixBytes);

            var bytes = new byte[8];
            random.NextBytes(bytes);
            var d2CSpanId = ByteArrayToHexStringConverter.Convert(bytes);
            var d2CCorrelationId = GenerateTraceId(prefix, d2CSpanId);

            random.NextBytes(bytes);
            var ingressSpanId = ByteArrayToHexStringConverter.Convert(bytes);
            var ingressCorrelationId = GenerateTraceId(prefix, ingressSpanId);

            var randDeviceName = deviceNames[random.Next(deviceNames.Length)];
            var d2cLog = GenerateD2CLogs(d2CCorrelationId, randDeviceName);
            var ingressLog = GenerateIngressLogs(ingressCorrelationId, d2CSpanId);

            random.NextBytes(bytes);
            var egressSpanId = ByteArrayToHexStringConverter.Convert(bytes);
            var egressCorrelationId = GenerateTraceId(prefix, egressSpanId);

            var randPointName = endpointNames[random.Next(endpointNames.Length)];
            var egressLog = GenerateEgressLogs(egressCorrelationId, ingressSpanId, randPointName);

            eventhubMessage.records.Add(d2cLog);
            eventhubMessage.records.Add(ingressLog);
            eventhubMessage.records.Add(egressLog);

            if (SendThirdPartyLogs)
            {
                random.NextBytes(bytes);
                var thirdPartyServiceD2CSpanId = ByteArrayToHexStringConverter.Convert(bytes);
                var thirdPartyServiceD2CCorrelationId = GenerateTraceId(prefix, thirdPartyServiceD2CSpanId);

                var randIndex = random.Next(deviceNames.Length);
                var randEndpoint = endpointNames[randIndex];
                var randService = serviceNames[randIndex];

                var thirdPartyServiceD2CLog = GenerateThirdPartyD2CLogs(thirdPartyServiceD2CCorrelationId, randService, randEndpoint);

                random.NextBytes(bytes);
                var thirdPartyServiceIngressSpanId = ByteArrayToHexStringConverter.Convert(bytes);
                var thirdPartyServiceIngressCorrelationId = GenerateTraceId(prefix, thirdPartyServiceIngressSpanId);
                var thirdPartyServiceIngressLog = GenerateThirdPartyIngressLogs(thirdPartyServiceIngressCorrelationId, thirdPartyServiceD2CSpanId, randService);

                eventhubMessage.records.Add(thirdPartyServiceD2CLog);
                eventhubMessage.records.Add(thirdPartyServiceIngressLog);
            }

            message = JsonConvert.SerializeObject(eventhubMessage);
            eventHubClient.Send(new EventData(Encoding.UTF8.GetBytes(message)));
        }

        static void SendingRandomMessages()
        {
            var eventHubClient = EventHubClient.CreateFromConnectionString(connectionString, eventHubName);
            var count = 1;
            while (true)
            {
                try
                {
                    Console.WriteLine("{0} > Sending message: {1}", DateTime.Now, count);
                    SendRandomLogs(eventHubClient);
                }
                catch (Exception exception)
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("{0} > Exception: {1}", DateTime.Now, exception.Message);
                    Console.ResetColor();
                }

                Thread.Sleep(5000);
                count++;
            }
        }
    }
}
