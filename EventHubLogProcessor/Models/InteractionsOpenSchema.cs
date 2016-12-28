using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Microsoft.AzureCAT.Extensions.Logging.AppInsights.Models
{
	/// <summary>
	/// OpenSchema are custom schemas sent to AppIngights
	/// Json ordering is critical for error case when overall size is too big
	///  but only first 1000 bytes are written as error
	/// </summary>
	public class InteractionsOpenSchema : BaseOpenSchema
    {
		[JsonProperty(Order = 100)]
		public int DurationInMs { get; set; }
		[JsonProperty(Order = 101)]
		public string Happiness { get; set; }
		[JsonProperty(Order = 102)]
		public string HappinessExplanation { get; set; }
		[JsonProperty(Order = 103)]
		public string RobotName { get; set; }
    }
}
