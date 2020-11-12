using System;
using System.Collections.Generic;
using System.Fabric;
using Fix;
using Microsoft.Azure.ServiceBus;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Microsoft.ServiceFabric.Services.Runtime;
using ServiceFabric.ServiceBus.Services.Netstd;
using ServiceFabric.ServiceBus.Services.Netstd.CommunicationListeners;

namespace MessagePump
{
    internal sealed class MessagePump : StatelessService
    {
        public MessagePump(StatelessServiceContext serviceContext) : base(serviceContext)
        {
        }

        protected override IEnumerable<ServiceInstanceListener> CreateServiceInstanceListeners()
        {
            var serviceBusTopicName = "<topic>";
            var serviceBusSubscriptionName = "<subscription>";
            var connectionString = "<connection string>";

            yield return CreateSubscriptionServiceInstanceListener(
                serviceBusTopicName,
                serviceBusSubscriptionName,
                connectionString);
        }

        private ServiceInstanceListener CreateSubscriptionServiceInstanceListener(
            string serviceBusTopicName,
            string serviceBusSubscriptionName,
            string connectionString)
        {
            return new ServiceInstanceListener(
                context => CreateServiceBusSubscriptionCommunicationListener(
                    serviceBusTopicName,
                    serviceBusSubscriptionName,
                    connectionString,
                    context),
                serviceBusTopicName);
        }

        protected ConfigurableServiceBusSubscriptionCommunicationListener CreateServiceBusSubscriptionCommunicationListener(
            string serviceBusTopicName,
            string serviceBusSubscriptionName,
            string connectionString,
            StatelessServiceContext context)
        {
            Func<IServiceBusCommunicationListener, IServiceBusMessageReceiver> receiverFactory =
                x => new ServiceFabricMessageReceiver(x);
            return new ConfigurableServiceBusSubscriptionCommunicationListener(
                receiverFactory,
                context,
                serviceBusTopicName,
                serviceBusSubscriptionName,
                connectionString,
                connectionString)
            {
                AutoComplete = false,
                MaxConcurrentCalls = 1,
                MessagePrefetchCount = 0,
                RetryPolicy = RetryPolicy.Default
            };
        }
    }
}