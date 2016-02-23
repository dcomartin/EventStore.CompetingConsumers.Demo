using System;
using System.Net;
using System.Text;
using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;

namespace PersistentSubscription
{
    /*
     * This example sets up a persistent subscription to a test stream.
     * 
     * As written it will use the default ipaddress (loopback) and the default tcp port 1113 of the event
     * store. In order to run the application bring up the event store in another window (you can use
     * default arguments eg EventStore.ClusterNode.exe) then you can run this application with it. Once 
     * this program is running you can run the WritingEvents sample to write some events to the stream
     * and they will appear over the persistent subscription. You can also run many concurrent instances of this
     * program and only one instance will receive the event.
     * 
     */
    class Program
    {
        private static void Main()
        {
            var subscription = new PersistentSubscriptionClient();
            subscription.Start();
        }
    }

    public class PersistentSubscriptionClient
    {
        private IEventStoreConnection _conn;
        private const string STREAM = "a_test_stream";
        private const string GROUP = "a_test_group";
        private const int DEFAULTPORT = 1113;
        private static readonly UserCredentials User = new UserCredentials("admin", "changeit");

        public void Start()
        {
            //uncommet to enable verbose logging in client.
            var settings = ConnectionSettings.Create(); //.EnableVerboseLogging().UseConsoleLogger();

            using (_conn = EventStoreConnection.Create(settings, new IPEndPoint(IPAddress.Loopback, DEFAULTPORT)))
            {
                _conn.ConnectAsync().Wait();

                var bufferSize = 10;
                var autoAck = true;

                CreateSubscription();

                _conn.ConnectToPersistentSubscription(STREAM, GROUP, EventAppeared, SubscriptionDropped, User,
                    bufferSize, autoAck);

                Console.WriteLine("waiting for events. press enter to exit");
                Console.ReadLine();
            }
        }

        /*
        * Normally the creating of the subscription group is not done in your general executable code. 
        * Instead it is normally done as a step during an install or as an admin task when setting 
        * things up. You should assume the subscription exists in your code.
        */
        private void CreateSubscription()
        {
            PersistentSubscriptionSettings settings = PersistentSubscriptionSettings.Create()
                .DoNotResolveLinkTos()
                .StartFromCurrent();

            try
            {
                _conn.CreatePersistentSubscriptionAsync(STREAM, GROUP, settings, User).Wait();
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException.GetType() != typeof (InvalidOperationException)
                    && ex.InnerException?.Message != $"Subscription group {GROUP} on stream {STREAM} already exists")
                {
                    throw;
                }
            }
        }

        // If the subscription is dropped reconnect.
        private void SubscriptionDropped(EventStorePersistentSubscriptionBase eventStorePersistentSubscriptionBase,
            SubscriptionDropReason subscriptionDropReason, Exception ex)
        {
            _conn.ConnectAsync().Wait();
        }

        private static void EventAppeared(EventStorePersistentSubscriptionBase eventStorePersistentSubscriptionBase,
            ResolvedEvent resolvedEvent)
        {
            var data = Encoding.ASCII.GetString(resolvedEvent.Event.Data);
            Console.WriteLine("Received: " + resolvedEvent.Event.EventStreamId + ":" + resolvedEvent.Event.EventNumber);
            Console.WriteLine(data);
        }
    }
}
