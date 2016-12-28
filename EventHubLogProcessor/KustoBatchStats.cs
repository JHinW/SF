using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EventHubLogProcessor
{
	public class KustoBatchStats
	{
		public int DocumentCount { get; private set; }
		public long MaxSerializationTimeMs { get; private set; }
		public long MinSerializationTimeMs { get; private set; }
		public long TotalSerializationTimeMs { get; private set; }

		public int Errors { get; private set; }


		public void ReportSerialization(long serializationTime, Exception e)
		{
			DocumentCount++;
			MaxSerializationTimeMs = Math.Max(MaxSerializationTimeMs, serializationTime);
			MinSerializationTimeMs = Math.Min(MinSerializationTimeMs, serializationTime);
			TotalSerializationTimeMs += serializationTime;
			if (e != null)
			{
				Errors++;
			}
		}

		public void Clear()
		{
			DocumentCount = 0;
			MaxSerializationTimeMs = 0;
			MinSerializationTimeMs = long.MaxValue;
			TotalSerializationTimeMs = 0;
			Errors = 0;
		}

		public string ToJsonPoperties()
		{
			StringBuilder sb = new StringBuilder();
			sb.AppendFormat("\"DocumentCount\" : {0},", DocumentCount);
			sb.AppendFormat("\"MaxSerializationTimeMs\" : {0},", MaxSerializationTimeMs);
			sb.AppendFormat("\"MinSerializationTimeMs\" : {0},", MinSerializationTimeMs);
			sb.AppendFormat("\"AvgSerializationTimeMs\" : {0},", DocumentCount == 0 ? 0 : TotalSerializationTimeMs / DocumentCount);
			sb.AppendFormat("\"Errors\" : {0},", Errors);
			return sb.ToString();
		}
	}
}
