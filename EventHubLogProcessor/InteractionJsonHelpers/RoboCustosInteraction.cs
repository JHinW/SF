using System;
using System.Collections.Generic;
using System.Linq;

namespace EventHubLogProcessor.InteractionJsonHelpers
{
	/// <summary>
	///   An interaction in RoboCustos
	/// </summary>
	public abstract class RoboCustosInteraction
	{
		/// <summary>
		///   A "." separated path, e.g. Interaction.RegisterAndLogin.VisitHomePage.ViewPage
		/// </summary>
		public abstract string Path { get; }

		/// <summary>
		///   The happiness grade of this interaction.
		/// </summary>
		public abstract InteractionHappinessGrade HappinessGrade { get; }

		/// <summary>
		///		The happiness explanation of this interaction.
		/// </summary>
		public abstract string HappinessExplanation { get; }

		/// <summary>
		///   The time this interaction was recorded.
		/// </summary>
		public abstract DateTimeOffset TimeInteractionRecorded { get; }

		/// <summary>
		///   The time this interaction took to perform.
		/// </summary>
		public abstract TimeSpan TimeTaken { get; }

		public string SimpleName
		{
			get { return Path.Split('.').Last(); }
		}

		/// <summary>
		///   If this is an Unacceptable interaction, the root cause (first innermost Unacceptable child)
		///   interaction that caused it to be such. Otherwise null.
		/// </summary>
		public RoboCustosInteraction RootCauseUnacceptableInteraction
		{
			get
			{
				if (HappinessGrade != InteractionHappinessGrade.Unacceptable)
				{
					return null;
				}
				return ChildInteractions
					.Select(p => p.RootCauseUnacceptableInteraction)
					.FirstOrDefault(i => i != null) ?? this;
			}
		}

		/// <summary>
		///   If this is a ReallyAnnoyed interaction, the root cause (first innermost ReallyAnnoyed child)
		///   interaction that caused it to be such. Otherwise null.
		/// </summary>
		public RoboCustosInteraction RootCauseReallyAnnoyedInteraction
		{
			get
			{
				if (HappinessGrade != InteractionHappinessGrade.ReallyAnnoyed)
				{
					return null;
				}
				return ChildInteractions
					.Select(p => p.RootCauseReallyAnnoyedInteraction)
					.FirstOrDefault(i => i != null) ?? this;
			}
		}

		/// <summary>
		/// Returns the root unnaceptable interaction if it exists, otherwise root really annoyed, otherwise null.
		/// </summary>
		public RoboCustosInteraction LikelyRootCauseInteraction
			=> RootCauseUnacceptableInteraction ?? RootCauseReallyAnnoyedInteraction;


		/// <summary>
		///   The child interactions.
		/// </summary>
		public abstract IEnumerable<RoboCustosInteraction> ChildInteractions { get; }

		/// <summary>
		///   In a DFS-ordering of the interaction tree (pre-order), return the last one that qualifies the given predicate.
		/// </summary>
		/// <param name="predicate">The predicate to evaluate.</param>
		/// <returns>The qualifying interaction or <c>null</c> if non qualify.</returns>
		public RoboCustosInteraction LastOrDefaultDFS(Predicate<RoboCustosInteraction> predicate)
		{
			var lastChild = ChildInteractions
				.Select(i => i.LastOrDefaultDFS(predicate))
				.LastOrDefault(i => i != null);
			if (lastChild != null)
			{
				return lastChild;
			}
			return predicate(this) ? this : null;
		}

		/// <summary>
		///   Gets the Details property with this path.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="path"></param>
		/// <returns></returns>
		public abstract T GetDetailPropertyByPath<T>(params string[] path);

		/// <summary>
		///   Gets the Details property of this name.
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="propertyName"></param>
		/// <returns></returns>
		public abstract T GetDetailProperty<T>(string propertyName);

		/// <summary>
		///   Checks if we have a Details property of this name.
		/// </summary>
		/// <param name="propertyName"></param>
		/// <returns></returns>
		public abstract bool HasDetailProperty(string propertyName);

		/// <summary>
		///   Gets the details property of this name, if it doesn't exist return default value
		/// </summary>
		/// <typeparam name="T"></typeparam>
		/// <param name="propertyName">Name of the property.</param>
		/// <param name="defaultValue">The default value.</param>
		/// <returns></returns>
		public T GetDetailProperty<T>(string propertyName, T defaultValue)
		{
			return HasDetailProperty(propertyName) ? GetDetailProperty<T>(propertyName) : defaultValue;
		}
	}
}
