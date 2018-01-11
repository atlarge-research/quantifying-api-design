package com.atlarge.opendc.simulator

/**
 * A bootstrapping interface for a conceptual model that is a logical representation of some system of entities,
 * relationships and processes, as a basis for simulations.
 *
 * @param M The shape of the model that is bootstrapped.
 * @author Fabian Mastenbroek (f.s.mastenbroek@student.tudelft.nl)
 */
interface Bootstrap<M> {
	/**
	 * Bootstrap a model `M` for a kernel in the given context.
	 *
	 * @param context The context to bootstrap to model in.
	 * @return The initialised model for the simulation.
	 */
	fun bootstrap(context: Context<M>): M

	/**
	 * A context for the bootstrap of some model type `M` that allows the model to register the entities of the model to
	 * the simulation kernel.
	 *
	 * @author Fabian Mastenbroek (f.s.mastenbroek@student.tudelft.nl)
	 */
	interface Context<out M> {
		/**
		 * Register the given entity to the simulation kernel.
		 *
		 * @param entity The entity to register.
		 * @return `true` if the entity had not yet been registered, `false` otherwise.
		 */
		fun register(entity: Entity<*, M>): Boolean

		/**
		 * Deregister the given entity from the simulation kernel.
		 *
		 * @param entity The entity to deregister.
		 * @return `true` if the entity had not yet been unregistered, `false` otherwise.
		 */
		fun deregister(entity: Entity<*, M>): Boolean

		/**
		 * Schedule a message for processing by a [Context].
		 *
		 * @param message The message to schedule.
		 * @param destination The destination of the message.
		 * @param sender The sender of the message.
		 * @param delay The amount of time to wait before processing the message.
		 */
		fun schedule(message: Any, destination: Entity<*, *>, sender: Entity<*, *>? = null, delay: Duration = 0)
	}

	companion object {
		/**
		 * Create a [Bootstrap] procedure using the given block to produce a bootstrap for a model of type `M`.
		 *
		 * @param block The block to produce the bootstrap.
		 * @return The bootstrap procedure that has been built.
		 */
		fun <M> create(block: (Context<M>) -> M): Bootstrap<M> = object : Bootstrap<M> {
			override fun bootstrap(context: Context<M>) = block(context)
		}
	}
}
