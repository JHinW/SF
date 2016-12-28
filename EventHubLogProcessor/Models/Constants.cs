using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Microsoft.AzureCAT.Extensions.Logging.AppInsights.Models
{
	public static class Constants
	{
		public enum DataSources
		{
			Interactions,
			TimedOperation,
			Log,
			EventTraces,
			Count,
			HTTP,
			PerfCounters,
			Exception
		}

		public static class LogTypes
		{
			public const string HttpLog = "HttpOpenSchema";
			public const string Exceptions = "ExceptionsOpenSchema";
			public const string Count = "CountOpenSchema";
			public const string Interactions = "InteractionsOpenSchema";
			public const string TimedOperation = "TimedOperationOpenSchema";
			public const string Log = "LogOpenSchema";
		}

		public static class TelemetryProps
		{
			public const string Kusto = "Kusto";
			public const string TrueString = "true";
			public const string CategoryName = "CategoryName";
			public const string Level = "Level";
			public const string DataSourceSchema = "DataSourceSchema";
			public const string FormattedMessage = "FormattedMessage";
			public const string UnknownFormat = "???";
			public const string Id = "Id";
			public const string Name = "Name";
			public const string CorrelationId = "CorrelationId";
			public const string SecondaryCorrelationId = "SecondaryCorrelationId";
			public const string Environment = "Environment";
			public const string MachineRole = "MachineRole";
			public const string MachineName = "MachineName";
			public const string MessageTemplate = "MessageTemplate";
			public const string CustomPropertyPrefix = "customProperty___";
			public const string Blob = "Blob";
			public const string Happiness = "Happiness";
			public const string HappinessExplanation = "HappinessExplanation";
			public const string RobotName = "RobotName";
			public const string DurationInMs = "DurationInMs";
			public const string MetricAggregation = "MetricAggregation";
		}

		public static class KustoProps
		{
			public const string CorrelationId = "CorrelationId";
			public const string SecondaryCorrelationId = "SecondaryCorrelationId";
			public const string Level = "Level";
			public const string Environment = "Environment";
			public const string MachineRole = "MachineRole";
			public const string MachineName = "MachineName";
			public const string ApplicationName = "ApplicationName";
			public const string MessageName = "MessageName";
			public const string Message = "Message";
			public const string MessageTemplate = "MessageTemplate";
		}

		public static class MetricProps
		{
			public const string Avg = "Avg";
			public const string Min = "Min";
			public const string Max = "Max";
			public const string Count = "Count";
			public const string StdDev = "StdDev";
			public const string P50 = "P50";
			public const string P90 = "P90";
			public const string P95 = "P95";
			public const string P99 = "P99";
		}

		public static Dictionary<string, string> KustoMappings = new Dictionary<string, string>
		{
			{ KustoProps.CorrelationId, TelemetryProps.CorrelationId},
			{ KustoProps.SecondaryCorrelationId, TelemetryProps.SecondaryCorrelationId},
			{ KustoProps.Level, TelemetryProps.Level},
			{ KustoProps.Environment, TelemetryProps.Environment},
			{ KustoProps.MachineRole, TelemetryProps.MachineRole},
			{ KustoProps.MachineName, TelemetryProps.MachineName},
			{ KustoProps.ApplicationName, TelemetryProps.CategoryName},
			{ KustoProps.MessageName, TelemetryProps.DataSourceSchema},
			{ KustoProps.Message, TelemetryProps.FormattedMessage},
			{ KustoProps.MessageTemplate, TelemetryProps.MessageTemplate},
		};
	}
}
