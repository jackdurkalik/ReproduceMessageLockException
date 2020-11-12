using System;
using System.Collections.Generic;
using System.Fabric;
using System.Linq;

using Microsoft.Azure.ServiceBus;

using ServiceFabric.ServiceBus.Services.Netstd;
using ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners;

namespace Fix
{
    public class ConfigurableServiceBusSubscriptionCommunicationListener : ServiceBusSubscriptionCommunicationListener
    {
        public ConfigurableServiceBusSubscriptionCommunicationListener(
            IServiceBusMessageReceiver receiver,
            ServiceContext context,
            string serviceBusTopicName,
            string serviceBusSubscriptionName,
            string serviceBusSendConnectionString,
            string serviceBusReceiveConnectionString)
            : base(
                receiver,
                context,
                serviceBusTopicName,
                serviceBusSubscriptionName,
                serviceBusSendConnectionString,
                serviceBusReceiveConnectionString)
        {
        }

        public ConfigurableServiceBusSubscriptionCommunicationListener(
            Func<IServiceBusCommunicationListener, IServiceBusMessageReceiver> receiverFactory,
            ServiceContext context,
            string serviceBusTopicName,
            string serviceBusSubscriptionName,
            string serviceBusSendConnectionString,
            string serviceBusReceiveConnectionString)
            : base(
                receiverFactory,
                context,
                serviceBusTopicName,
                serviceBusSubscriptionName,
                serviceBusSendConnectionString,
                serviceBusReceiveConnectionString)
        {
        }

        public bool AutoComplete { get; set; } = false;

        protected override void ListenForMessages()
        {
            var messageHandlerOptions = new MessageHandlerOptions(this.ExceptionReceivedHandler);
            if (this.AutoRenewTimeout.HasValue)
            {
                messageHandlerOptions.MaxAutoRenewDuration = this.AutoRenewTimeout.Value;
            }

            if (this.MaxConcurrentCalls.HasValue)
            {
                messageHandlerOptions.MaxConcurrentCalls = this.MaxConcurrentCalls.Value;
            }

            // this does nothings
            //messageHandlerOptions.AutoComplete = this.AutoComplete;
            this.ServiceBusClient.RegisterMessageHandler(this.ReceiveMessageAsync, messageHandlerOptions);
        }
    }
}