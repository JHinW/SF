using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubLogProcessor
{
	public static class EventProcessorConstants
	{
		public const string SerilogEventType = "SerilogEvent";
		public const string RoboCustosInteractionEventType = "RoboCustosInteraction";
		public const string ExternalTelemetryEventType = "ExternalTelemetry";
		public const string AzureResourcesEventType = "azure-resources";
	}
}
