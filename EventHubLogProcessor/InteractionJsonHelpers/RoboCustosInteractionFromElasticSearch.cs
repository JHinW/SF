using System;
using System.Collections.Generic;
using System.Linq;
using Nest;
using Newtonsoft.Json.Linq;

namespace EventHubLogProcessor.InteractionJsonHelpers
{
	public sealed class RoboCustosInteractionFromElasticSearch : RoboCustosInteraction
	{
		private readonly JObject _interaction;
		private readonly string _path;

		public RoboCustosInteractionFromElasticSearch(string path, JObject interaction)
		{
			_path = path;
			_interaction = interaction;
		}

		public override string Path
		{
			get { return _path; }
		}

		public override InteractionHappinessGrade HappinessGrade
		{
			get
			{
				return (InteractionHappinessGrade)Enum.Parse(typeof (InteractionHappinessGrade),
					_interaction.Property("HappinessGrade").Value.Value<string>());
			}
		}

		public override string HappinessExplanation => _interaction.Property("HappinessExplanation")?.Value.Value<string>();

		public override DateTimeOffset TimeInteractionRecorded => new DateTimeOffset(_interaction.Property("TimeInteractionRecorded").Value.Value<DateTime>());

		public override TimeSpan TimeTaken
		{
			get
			{
				var timeTakenValue = _interaction.Property("TimeTaken")?.Value.Value<string>();
				if (string.IsNullOrWhiteSpace(timeTakenValue))
				{
					return TimeSpan.Zero;
				}
				try
				{
					var timeTaken = TimeSpan.Parse(timeTakenValue);
					return timeTaken;
				}
				catch (Exception)
				{
					// Any failure to parse  
					return TimeSpan.Zero;
				}
			}
		}

		public override IEnumerable<RoboCustosInteraction> ChildInteractions
		{
			get
			{
				if (_interaction.Properties().Any(p => p.Name == "Components"))
				{
					return _interaction
						.Property("Components")
						.Value
						.Value<JArray>()
						.Where(IsInteractionJson)
						.Select((c, i) => new RoboCustosInteractionFromElasticSearch(_path + "." + i, (JObject)c));
				}
				return _interaction
					.Properties()
					.Where(p => IsInteractionJson(p.Value))
					.Select(p => new RoboCustosInteractionFromElasticSearch(_path + "." + p.Name, (JObject)p.Value));
			}
		}

		private JObject Details
		{
			get
			{
				if (!_interaction.Properties().Any(p => p.Name == "Details"))
				{
					return null;
				}
				return _interaction.Property("Details").Value.Value<JObject>();
			}
		}

		public override T GetDetailPropertyByPath<T>(params string[] path)
		{
			var node = Details;
			for (int i = 0; i < path.Length - 1; i++)
			{
				node = node.Property(path[i]).Value.Value<JObject>();
			}
			return node.Property(path[path.Length - 1]).Value.Value<T>();
		}

		public override T GetDetailProperty<T>(string propertyName)
		{
			return Details.Property(propertyName).Value.Value<T>();
		}

		public override bool HasDetailProperty(string propertyName)
		{
			return Details != null &&
			       Details.Properties().Any(p => string.Equals(p.Name, propertyName, StringComparison.InvariantCulture));
		}

		private static bool IsInteractionJson(JToken json)
		{
			var asJObject = json as JObject;
			return asJObject != null &&
			       asJObject.Properties().Any(p => p.Name == "HappinessGrade") &&
						 asJObject.Properties().Any(p => p.Name == "TimeInteractionRecorded");
		}
	}
}
