using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubLogProcessor
{
	/// <summary>
	/// Represents an invalid EventHub event data, e.g. due to missing properties or unknown property types.
	/// </summary>
	public sealed class InvalidEventDataException : Exception
	{
		public InvalidEventDataException(string invalidReason) : base(invalidReason)
		{
		}
	}
}
