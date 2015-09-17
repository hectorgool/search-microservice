package controllers


import com.twitter.bijection.twitter_util._
import com.twitter.io.Charsets
import lib._
import org.jboss.netty.util.CharsetUtil
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.{Json,JsValue}
import play.api.libs.json.Json.{toJson}
import play.api.mvc._
import play.api.mvc.MultipartFormData._
import play.api.Play.current
import play.api._
import scala.concurrent.{Future}
import scala.util.{Failure, Success}


class Application extends Controller with UtilBijections {


	val elasticsearchIndex = current.configuration.getString("elasticsearch.index").get
	val indexType = current.configuration.getString("elasticsearch.indexType").get
	val size = current.configuration.getString("elasticsearch.size").get

  	def index = Action {
    	Ok(views.html.index("Your API is ready."))
  	}

	def searchDoc = Action.async(parse.json) { request =>
 
		val term = (request.body \ "term").as[String]
	
		val json: JsValue = Json.obj(
			"size" -> size,
			"query" -> Json.obj(
				"match" -> Json.obj(
					"_all" -> Json.obj(
						"query" -> term,
                		"operator" -> "and"	
					)
				)
			),
			"sort" -> Json.arr(
				Json.obj(
					"colonia" -> Json.obj( "order"-> "asc", "mode" -> "avg")
				)
			)
		)

		val futureScala = twitter2ScalaFuture.apply( FinagleClient.documentSearch( elasticsearchIndex, indexType, json ) )
        
		futureScala.map( f => 
			Ok( Json.parse( f.getContent.toString( CharsetUtil.UTF_8 ) ) )
		)		

	}


}
