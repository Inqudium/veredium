package eu.inqudium.veredium.testsupport

/**
 * The response body of the client tests. A no-arg constructor and a mutable property, because the clients' Jackson
 * mapper has no Kotlin module. Assumption: without it, Jackson (3.1.7) binds this class through the no-arg constructor
 * and the setter.
 */
class Greeting {
    var message: String? = null
}

/** The request body of the client tests' POSTs; serialises to `{"id":...}` through its bean getter. */
class LookupRequest(
    var id: String? = null,
)

/** A body Jackson cannot serialise: the accessor it must call to read the value throws. */
class Unserialisable {
    val value: String get() = error("this body cannot be serialised")
}
