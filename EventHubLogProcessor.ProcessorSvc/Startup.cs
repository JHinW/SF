using System.Web.Http;
using Microsoft.Owin.StaticFiles;
using Owin;

namespace EventHubLogProcessor.ProcessorSvc
{
	public static class Startup
	{
		// This code configures Web API. The Startup class is specified as a type
		// parameter in the WebApp.Start method.
		public static void ConfigureApp(IAppBuilder app)
		{
			// Configure Web API for self-host. 
			HttpConfiguration config = new HttpConfiguration();
			config.MapHttpAttributeRoutes();
			app.UseWebApi(config);

			// Html
			app.UseDefaultFiles(new DefaultFilesOptions
			{
				DefaultFileNames = new [] { "default.html" },
			});
			app.UseStaticFiles();
		}
	}
}
