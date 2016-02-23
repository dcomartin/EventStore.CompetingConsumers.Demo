using System;
using System.Net;
using System.Text;
using EventStore.ClientAPI;

namespace WritingEvents
{
    /*
     * This example writes 100 events to the event store. The events will be written to the stream STREAM.
     * 
     * As written it will use the default ipaddress (loopback) and the default tcp port 1113 of the event
     * store. In order to run the application bring up the event store in another window (you can use
     * default arguments eg EventStore.ClusterNode.exe) then you can run this application with it.
     * 
     * The events are in the form of
     * {
     *     somedata : <incrementinginteger>
     * }
     * 
     * with metadata of:
     * {
     *     metadata : <incrmentinginteger>
     * }
     */
    class Program
    {
        static void Main(string[] args)
        {
            const string STREAM = "a_test_stream";
            const int DEFAULTPORT = 1113;
            //uncomment to enable verbose logging in client.
            var settings = ConnectionSettings.Create();//.EnableVerboseLogging().UseConsoleLogger();
            using (var conn = EventStoreConnection.Create(settings, new IPEndPoint(IPAddress.Loopback, DEFAULTPORT)))
            {
                conn.ConnectAsync().Wait();
                for (var x = 0; x < 100; x++)
                {
                    conn.AppendToStreamAsync(STREAM,
                        ExpectedVersion.Any,
                        GetEventDataFor(x)).Wait();
                    Console.WriteLine("event " + x + " written.");
                }
            }
        }

        private static EventData GetEventDataFor(int i)
        {
            return new EventData(
                Guid.NewGuid(),
                "eventType",
                true,
                Encoding.ASCII.GetBytes("{'somedata' : " + i + "}"),
                Encoding.ASCII.GetBytes("{'metadata' : " + i + "}")
                );
        }
    }
}
