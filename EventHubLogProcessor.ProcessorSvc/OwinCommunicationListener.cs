using System;
using System.Collections;
using System.Collections.Generic;
using System.Fabric;
using System.Fabric.Description;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Owin.Hosting;
using Microsoft.ServiceFabric.Services.Communication.Runtime;
using Serilog;
using E2ECommon.Logging;
using Owin;

namespace EventHubLogProcessor.ProcessorSvc
{
	/// <summary>
	/// Owin-based Service Fabric communication listener.
	/// </summary>
	public class OwinCommunicationListener : ICommunicationListener
	{
		private Dictionary<string, object> AdditionalLogProperties => new Dictionary<string, object>
				{
						{ "ServiceName", _serviceContext.ServiceName },
						{ "ReplicaId", _serviceContext.ReplicaOrInstanceId },
						{ "PartitionId", _serviceContext.PartitionId },
						{ "NodeId", _serviceContext.NodeContext?.NodeId },
						{ "NodeName", _serviceContext.NodeContext?.NodeName },
				};

		private readonly Action<IAppBuilder> _startup;
		private readonly string _appRoot;
		private IDisposable _serverHandle;
		private string _listeningAddress;
		private readonly ServiceContext _serviceContext;

		public OwinCommunicationListener(string appRoot, Action<IAppBuilder> startup, ServiceContext serviceContext)
		{
			_startup = startup;
			_appRoot = appRoot;
			_serviceContext = serviceContext;
		}

		public Task<string> OpenAsync(CancellationToken cancellationToken)
		{
			try
			{
				EndpointResourceDescription serviceEndpoint = _serviceContext.CodePackageActivationContext.GetEndpoint("ServiceEndpoint");
				int port = serviceEndpoint.Port;

				if (_serviceContext is StatelessServiceContext)
				{
					_listeningAddress = String.Format(
							CultureInfo.InvariantCulture,
							"http://+:{0}/{1}",
							port,
							string.IsNullOrWhiteSpace(_appRoot)
									? string.Empty
									: _appRoot.TrimEnd('/') + '/');
				}
				else
				{
					_listeningAddress = String.Format(
							CultureInfo.InvariantCulture,
							"http://+:{0}/{1}/{2}/{3}",
							port,
							((StatefulServiceContext)_serviceContext).PartitionId,
							((StatefulServiceContext)_serviceContext).ReplicaId,
							string.IsNullOrWhiteSpace(_appRoot)
									? string.Empty
									: _appRoot.TrimEnd('/') + '/');
				}

				Log.Information("OwinCommunicationListener.OpenAsync listening on {Endpoint}. ServiceName={ServiceName}, ReplicaId={ReplicaId}, PartitionId={PartitionId}, NodeId={NodeId}, NodeName={NodeName}",
						_listeningAddress,
						_serviceContext.ServiceName,
						_serviceContext.ReplicaOrInstanceId,
						_serviceContext.PartitionId,
						_serviceContext.NodeContext?.NodeId,
						_serviceContext.NodeContext?.NodeName);

				_serverHandle = WebApp.Start(_listeningAddress, appBuilder => _startup.Invoke(appBuilder));
				string publishAddress = _listeningAddress.Replace("+", FabricRuntime.GetNodeContext().IPAddressOrFQDN);
				return Task.FromResult(publishAddress);
			}
			catch (Exception e)
			{
				e.LogException("OwinCommunicationListener.OpenAsync", AdditionalLogProperties);
				throw;
			}
		}

		public Task CloseAsync(CancellationToken cancellationToken)
		{
			this.StopWebServer("OwinCommunicationListener.CloseAsync");

			return Task.FromResult(true);
		}

		public void Abort()
		{
			this.StopWebServer("OwinCommunicationListener.Abort");
		}

		private void StopWebServer(string context)
		{
			if (_serverHandle != null)
			{
				try
				{
					_serverHandle.Dispose();
				}
				catch (ObjectDisposedException)
				{
					// no-op
				}
				catch (Exception e)
				{
					e.LogException(context, AdditionalLogProperties);
					throw;
				}
			}
		}
	}
}
