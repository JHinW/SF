using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.AzureCAT.Extensions.Logging.AppInsights.Models
{
	public interface IBaseOpenSchema
	{
		string CorrelationId { get; set; }
		string SecondaryCorrelationId { get; set; }
		DateTime Timestamp { get; set; }
		string MessageId { get; set; }
		string Level { get; set; }
		string Environment { get; set; }
		string MachineRole { get; set; }
		string MachineName { get; set; }
		string ApplicationName { get; set; }
		string MessageName { get; set; }
		string Blob { get; set; }
	}
}
