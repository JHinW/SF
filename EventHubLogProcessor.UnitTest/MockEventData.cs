using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace EventHubLogProcessor.UnitTest
{
	public static class MockEventData
	{
		public static EventData CreateSerilogEventData(string body)
		{
			return CreateSerilogEventData(Guid.NewGuid().ToString(), DateTime.UtcNow, body);
		}

		public static EventData CreateSerilogEventData(string messageId, DateTime timestamp, string body)
		{
			var data = new EventData(Encoding.UTF8.GetBytes(body ?? string.Empty));
			data.Properties["Type"] = "SerilogEvent";
			data.Properties["MessageId"] = messageId;
			data.Properties["Timestamp"] = timestamp.ToString(CultureInfo.InvariantCulture);
			return data;
		}

		public static EventData CreateRoboCustosEventData(string body)
		{
			return CreateRoboCustosEventData(Guid.NewGuid().ToString(), DateTime.UtcNow, body);
		}

		public static EventData CreateRoboCustosEventData(string messageId, DateTime timestamp, string body)
		{
			var data = new EventData(Encoding.UTF8.GetBytes(body ?? string.Empty));
			data.Properties["Type"] = "RoboCustosInteraction";
			data.Properties["MessageId"] = messageId;
			data.Properties["Timestamp"] = timestamp.ToString(CultureInfo.InvariantCulture);
			return data;
		}

		public static EventData CreateExternalTelemetryEventData(string body)
		{
			return CreateExternalTelemetryEventData(Guid.NewGuid().ToString(), DateTime.UtcNow, body);
		}

		public static EventData CreateExternalTelemetryEventData(string messageId, DateTime timestamp, string body)
		{
			var data = new EventData(Encoding.UTF8.GetBytes(body ?? string.Empty));
			data.Properties["Type"] = "ExternalTelemetry";
			data.Properties["MessageId"] = messageId;
			data.Properties["Timestamp"] = timestamp.ToString(CultureInfo.InvariantCulture);
			return data;
		}

		public static EventData CreateAzureResourcesEventData(string body)
		{
			return CreateAzureResourcesEventData(Guid.NewGuid().ToString(), DateTime.UtcNow, body);
		}

		public static EventData CreateAzureResourcesEventData(string messageId, DateTime timestamp, string body)
		{
			var data = new EventData(Encoding.UTF8.GetBytes(body ?? string.Empty));
			data.Properties["Type"] = "azure-resources";
			data.Properties["Source"] = "Microsoft.Compute/availabilitySets";
			data.Properties["MessageId"] = messageId;
			data.Properties["Timestamp"] = timestamp.ToString(CultureInfo.InvariantCulture);
			return data;
		}

		public static EventData CreateInvalidEventData(string body)
		{
			return CreateInvalidEventData(Guid.NewGuid().ToString(), DateTime.UtcNow.ToString(CultureInfo.InvariantCulture), body);
		}

		public static EventData CreateInvalidEventData(string messageId, string timestamp, string body)
		{
			var data = new EventData(Encoding.UTF8.GetBytes(body ?? string.Empty));
			data.Properties["Type"] = "invalid";
			data.Properties["MessageId"] = messageId;
			data.Properties["Timestamp"] = timestamp.ToString(CultureInfo.InvariantCulture);
			return data;
		}
	}
}
