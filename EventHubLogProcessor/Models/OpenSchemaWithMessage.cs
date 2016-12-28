using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Microsoft.AzureCAT.Extensions.Logging.AppInsights.Models
{
	public abstract class OpenSchemaWithMessage : BaseOpenSchema
	{
		// Message can be large, make it one of the last properties
		[JsonProperty(Order = 200)]
		public string Message { get; set; }
		[JsonProperty(Order = 11)]
		public string MessageTemplate { get; set; }
	}
}
