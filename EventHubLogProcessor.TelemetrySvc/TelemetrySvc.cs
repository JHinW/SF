using System;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Query;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using E2ECommon.SystemMonitor;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Serilog;
using StatelessService = Microsoft.ServiceFabric.Services.Runtime.StatelessService;

namespace EventHubLogProcessor.TelemetrySvc
{
	/// <summary>
	/// An instance of this class is created for each service instance by the Service Fabric runtime.
	/// </summary>
	internal sealed class TelemetrySvc : StatelessService
	{
		public TelemetrySvc(StatelessServiceContext context)
				: base(context)
		{ }

		/// <summary>
		/// This is the main entry point for your service instance.
		/// </summary>
		/// <param name="cancellationToken">Canceled when Service Fabric needs to shut down this service instance.</param>
		protected override async Task RunAsync(CancellationToken cancellationToken)
		{
			// Get all the nodes in the cluster so we know what to ping.
			//
			List<string> nodeNames = new List<string>();

			NodeList nodes;
			using (var fc = new FabricClient())
			{
				nodes = await fc.QueryManager.GetNodeListAsync();
			}

			foreach (var node in nodes)
			{
				// the node name may not match the machine name (the name that the machine in the cluster calls itself) but we have the IP
				// and experiments have shown that in real clusters that seems to work.
				//
				try
				{
					var hostEntry = Dns.GetHostEntry(node.IpAddressOrFQDN);

					var machineName = hostEntry.HostName;

					// grab everything before the first dot (assuming that's the root machine name)
					//
					if (machineName.IndexOf('.') >= 0)
					{
						machineName = machineName.Substring(0, machineName.IndexOf('.'));
					}
					// Make sure it's a name not an IP and has not already been added
					//
					if (machineName.Length > 0 && !char.IsNumber(machineName[0]) && !nodeNames.Contains(machineName.ToUpperInvariant()))
					{
						nodeNames.Add(machineName.ToUpperInvariant());
					}
				}
				catch (Exception e)
				{
					// failure to resolve ip means we don't know the name so skip machine
					//
					Log.Error(e, "Exception processing node {NodeIpOrFQDN}", node.IpAddressOrFQDN);
				}
			}

			try
			{
				// TODO: interim implementation of telemetry service logged more things than the standard system monitor, we may we to add some more of that
				// capability into the re-usable one in future, see the PerfMonCounterLoader class to the details of those perfmon counters
				//

				// TODO: skip event log for now (that requires admin privs and we are not running as admin)
				//
				SystemMonitor monitor = new SystemMonitor(Log.Logger,
						SystemMonitorChecks.Heartbeat |
						SystemMonitorChecks.NetworkConnectivity |
						SystemMonitorChecks.PerfmonCounters,
						nodeNames);

				monitor.Run(cancellationToken);
			}
			catch (Exception e)
			{
				Log.Error(e, "TelemetrySvc.RunAsync");
				throw;
			}
		}
	}
}
