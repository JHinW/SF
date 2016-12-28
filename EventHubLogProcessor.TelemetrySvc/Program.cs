using System;
using System.Fabric;
using System.Threading;
using E2ECommon.Logging;
using Microsoft.ServiceFabric.Services.Runtime;
using Serilog;
using Serilog.Sinks.Elasticsearch;

namespace EventHubLogProcessor.TelemetrySvc
{
	internal static class Program
	{
		/// <summary>
		/// This is the entry point of the service host process.
		/// </summary>
		private static void Main()
		{
			try
			{
				// For now, we have to manually configure Serilog because E2ECommon.Logging because
				// it depends on Elasticsearch.Net 1.7, but we depend on Elasticsearc.Net 2.3.
				ConfigureSerilog();

				// The ServiceManifest.XML file defines one or more service type names.
				// Registering a service maps a service type name to a .NET type.
				// When Service Fabric creates an instance of this service type,
				// an instance of the class is created in this host process.

				ServiceRuntime.RegisterServiceAsync("TelemetrySvcType",
						context => new TelemetrySvc(context)).GetAwaiter().GetResult();

				// Prevents this host process from terminating so services keep running.
				Thread.Sleep(Timeout.Infinite);
			}
			catch (Exception e)
			{
				Log.Error(e, "TelemetrySvc.Main");
				throw;
			}
		}

		private static void ConfigureSerilog()
		{
			var settings = new AppConfigSettingsProvider();
			var config = new LoggerConfiguration();
			config = ConfigureForElasticSearch(config, settings.GetSettingValue("LogElasticSearchUri"), settings.GetSettingValue("LogElasticSearchUserName"), settings.GetSettingValue("LogElasticSearchPassword"));
			config = EnrichWithProperty(config, "Environment", settings.GetSettingValue("Environment"));
			config = EnrichWithProperty(config, "NodeName", FabricRuntime.GetNodeContext().NodeName);
			config = EnrichWithProperty(config, "MachineName", Environment.MachineName);
			Log.Logger = config.CreateLogger();
		}

		private static LoggerConfiguration ConfigureForElasticSearch(LoggerConfiguration config, string uri, string username, string password)
		{
			if (string.IsNullOrEmpty(uri))
				return config;

			var esLogUri = new Uri(uri);
			var esLogOptions = new ElasticsearchSinkOptions(esLogUri)
			{
				IndexFormat = "logstash-{0:yyyy.MM.dd}",
				TypeName = "logevent",
			};

			if (!string.IsNullOrEmpty(username) && !string.IsNullOrEmpty(password))
			{
				esLogOptions.ModifyConnectionSettings = c => c.BasicAuthentication(username, password);
			}

			return config.WriteTo.Elasticsearch(esLogOptions);
		}

		private static LoggerConfiguration EnrichWithProperty(LoggerConfiguration config, string propertyName, string propertyValue)
		{
			if (!string.IsNullOrEmpty(propertyValue))
			{
				config = config.Enrich.WithProperty(propertyName, propertyValue);
			}

			return config;
		}
	}
}
