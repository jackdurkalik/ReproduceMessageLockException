using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.ServiceBus;
using ServiceFabric.ServiceBus.Services.Netstd;
using ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners;

namespace MessagePump
{
    public class ServiceFabricMessageReceiver : DefaultServiceBusMessageReceiver
    {
        public ServiceFabricMessageReceiver(IServiceBusCommunicationListener listener)
            : base(listener, LogMessage)
        {
            // Remove this comment to fix it
            //this.AutoComplete = false;
        }

        private static void LogMessage(string message)
        {
            // Log message
            Console.WriteLine(message);
        }

        protected override async Task ReceiveMessageImplAsync(Message message, CancellationToken cancellationToken)
        {
            await Listener.Abandon(message);
            //throw new Exception();
        }

        protected override async Task<bool> HandleReceiveMessageError(Message message, Exception ex)
        {
            await Listener.Abandon(message);
            return true;
        }
    }
}