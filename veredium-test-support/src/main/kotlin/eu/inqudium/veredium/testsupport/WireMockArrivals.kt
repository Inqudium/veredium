package eu.inqudium.veredium.testsupport

import com.github.tomakehurst.wiremock.http.Request
import com.github.tomakehurst.wiremock.http.RequestListener
import com.github.tomakehurst.wiremock.http.Response
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CountDownLatch

/**
 * What a request to a URL triggers once it reached WireMock, per test - behind ONE listener per server. Assumption:
 * WireMock's `resetAll()` (3.13.2) does not remove a request listener, so one added per test would pile up and fire
 * for a later test's request to the same URL. Register it once with the server (`addMockServiceRequestListener`) and
 * [clear] it before every test.
 */
class WireMockArrivals : RequestListener {
    private val actions = ConcurrentHashMap<String, () -> Unit>()

    override fun requestReceived(
        request: Request,
        response: Response,
    ) {
        actions[request.url]?.invoke()
    }

    /** A latch that opens once a request to [url] reached WireMock in the current test. */
    fun arrivalOf(url: String): CountDownLatch = CountDownLatch(1).also { latch -> actions[url] = latch::countDown }

    /** Runs [action] whenever a request to [url] reaches WireMock in the current test. */
    fun onArrival(
        url: String,
        action: () -> Unit,
    ) {
        actions[url] = action
    }

    /** Forgets every action of the test before. */
    fun clear() = actions.clear()
}
