using Elasticsearch.Net;
using Microsoft.ServiceBus.Messaging;
using Nest;
using System;
using System.Diagnostics;

namespace EventHubLogProcessor
{
	public sealed class EsSinkEventProcessorFactory : IEventProcessorFactory
	{
		private readonly IElasticClient _client;
		private readonly bool _sendProcessingStats;

		public EsSinkEventProcessorFactory(Uri elasticSearchHost, string userName, string password, bool sendProcessingStats = true)
		{
			var config = new ConnectionSettings(elasticSearchHost);
			if (!string.IsNullOrEmpty(userName))
			{
				config = config.BasicAuthentication(userName, password);
			}
			if (Debugger.IsAttached)
			{
				// When the debugger is attached, enable diagnostic information on request/responses.
				config = config.DisableDirectStreaming();
			}

			_sendProcessingStats = sendProcessingStats;
			_client = new ElasticClient(config);
		}

		public IEventProcessor CreateEventProcessor(PartitionContext context)
		{
			return new EsSinkEventProcessor(_client, _sendProcessingStats);
		}
	}
}
