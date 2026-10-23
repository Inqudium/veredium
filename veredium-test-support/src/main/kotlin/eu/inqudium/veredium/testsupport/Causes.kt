package eu.inqudium.veredium.testsupport

/** [failure] and its causes, outermost first, stopping at a cause that is its own. */
fun causeChain(failure: Throwable?): List<Throwable> = generateSequence(failure) { it.cause.takeIf { cause -> cause !== it } }.toList()

/** The simple class names along [failure]'s cause chain - for an engine's exception whose type is shaded or optional. */
fun causeNames(failure: Throwable?): List<String> = causeChain(failure).map { it.javaClass.simpleName }
