using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Microsoft.AzureCAT.Extensions.Logging.AppInsights.Models
{
	/// <summary>
	/// OpenSchema are custom schemas sent to AppIngights
	/// Json ordering is critical for error case when overall size is too big
	///  but only first 1000 bytes are written as error
	/// </summary>
	public class LogOpenSchema : OpenSchemaWithMessage
	{
		[JsonProperty(Order = 100)]
		public bool UnknownMessage { get; set; }
    }
}
