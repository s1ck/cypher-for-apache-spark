package org.opencypher.memcypher

import org.opencypher.okapi.testing.BaseTestSuite

class MemCypherTestSuite extends BaseTestSuite {

  implicit val memCypher: MemCypherSession = MemCypherSession()

}