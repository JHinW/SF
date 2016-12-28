using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.AzureCAT.Extensions.Logging.AppInsights.Models;
using Microsoft.ServiceBus.Messaging;

namespace EventHubLogProcessor
{
	class DeserializationInfo
	{
		public BaseOpenSchema DeserializedEvent { get; }
		public string EventType { get; }
		public TimeSpan ElaspsedTime { get; }
		public Exception Exception { get; }

		public DeserializationInfo(BaseOpenSchema deserializedEvent, string eventType, TimeSpan elapsedTime, Exception e)
		{
			DeserializedEvent = deserializedEvent;
			EventType = eventType;
			ElaspsedTime = elapsedTime;
			Exception = e;
		}
	}
}
