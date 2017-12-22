package util

object Twitter {
	def initialize(): Unit = {
		System.setProperty("twitter4j.oauth.consumerKey", sys.env("TW_CON_KEY"))
		System.setProperty("twitter4j.oauth.consumerSecret", sys.env("TW_CON_SECRET"))
		System.setProperty("twitter4j.oauth.accessToken", sys.env("TW_TOKEN"))
		System.setProperty("twitter4j.oauth.accessTokenSecret", sys.env("TW_TOKEN_SECRET"))
	}
}
