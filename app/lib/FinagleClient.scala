package lib


import com.twitter.conversions.time._
import com.twitter.finagle.{Http, Service, ServiceFactory}
import com.twitter.util.Future
import org.jboss.netty.buffer.ChannelBuffers
import org.jboss.netty.handler.codec.http.HttpHeaders.Names._
import org.jboss.netty.handler.codec.http._
import org.jboss.netty.util.CharsetUtil
import org.jboss.netty.util.CharsetUtil._
import play.api.libs.json.{Json, JsValue}
import play.api.Logger
import play.api.Play.current


object FinagleClient{


  val hosts = current.configuration.getString("elasticsearch.hosts").get//conf/application.conf:elasticsearch.hosts="localhost:9200"
  val client: Service[HttpRequest, HttpResponse] = Http.newService(hosts)

  def requestBuilderGet(path: List[String], json: JsValue): DefaultHttpRequest = {

    val payload = ChannelBuffers.copiedBuffer( Json.stringify(json) , UTF_8)    
    val _path = path.mkString("/","/","")
    val request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, _path)
    
    request.headers().add(USER_AGENT, "Finagle - Play")
    request.headers().add(HOST, "http://localhost")
    request.headers().add(CONTENT_TYPE, "application/json")
    request.headers().add(CONNECTION, "keep-alive")
    request.headers().add(CONTENT_LENGTH, String.valueOf(payload.readableBytes()));
    request.setContent(payload)
    
    Logger.debug("Sending request:\n%s".format(request))    
    Logger.debug("Sending body:\n%s".format(request.getContent.toString(CharsetUtil.UTF_8)))
    
    request

  }

  def sendToElastic(request: DefaultHttpRequest): Future[HttpResponse] = {

    Logger.debug("Request to send is %s" format request)
    val httpResponse = client(request)

    httpResponse.onSuccess{
      response =>
        Logger.debug("Received response: " + response)
    }.onFailure{ err: Throwable =>
        Logger.error(err.toString)
    }

  }

  def documentSearch(index: String, indexType: String, json: JsValue): Future[HttpResponse] ={

    val req = requestBuilderGet(List( index, indexType, "_search"), json)
    sendToElastic(req)

  }


}