package fr.inrae.msd.rdf

import org.apache.spark.sql.{Dataset, Encoder, Encoders}

import scala.language.{existentials, postfixOps}
import scala.util.{Failure, Success, Try}

case object ClassyFireRequest {

  val durationRetry : Int   = 10000 /* milliseconds */
  val retryNum      : Int   = 10
  type ResultSetDirectParentAndAltPerents =  (Option[(String,String)],Option[(Seq[String],String)])

  def buildTriplesDirectParentAndAltParents(responseRequest : String,CID : String)
  : ResultSetDirectParentAndAltPerents = {

    val data = ujson.read(responseRequest)
    val tripleDirectParent =
      Try(data("direct_parent")("chemont_id").value.toString) match {
        case Success(uriDirectParent) => Some((uriDirectParent,CID))
        case Failure(_) => None
      }

    val listTriplesAltParents  =
      Try(data("alternative_parents").arr.map( r => r("chemont_id").value.toString))  match {
        case Success(listUrisAltParents) => Some(listUrisAltParents,CID)
        case Failure(_) => None
      }

    (tripleDirectParent,listTriplesAltParents)
  }

  /**
   * https://github.com/eMetaboHUB/Forum-DiseasesChem/blob/master/app/build/classyfire_functions.py#L49
   *
   * This function is used to send a query to  classyfire.wishartlab.com/entities/INCHIKEY.json to retrieve classiication result for a compound, given his InchiKey.
    This function return the classification is json format or False if there was an error. Logs and ids for which the request failed are reported in classyFire.log and classyFire_error_ids.log
    - CID: PubChem compound identifier (use for logs)
    - InchiKey: input inchikey
   * @param CID
   * @param InchiKey
   */
  def getEntityFromClassyFire(CID : String , inchiKey  : String) : Option[ResultSetDirectParentAndAltPerents] = {
    val p = requests.get(
      s"http://classyfire.wishartlab.com/entities/$inchiKey.json",
      headers=Map("Content-Type"-> "application/json")
    )

    Some(buildTriplesDirectParentAndAltParents(p.text,CID))
  }

  // Returning a Try[T] wrapper
  // Returning T, throwing the exception on failure
  @annotation.tailrec
   def retry[T](n: Int)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case Failure(e) if n > 0 => {
        println(e.getMessage)
        if (e.getMessage.contains(":404") || e.getMessage.contains("public/404.html")) {
          /* unkown INCHIKEY */
          throw new Exception("stop retry")
        }
        println(s"***RETRY $n")
        Thread.sleep(durationRetry)
        retry(n - 1)(fn)
      }
      case Failure(e) => println(e.getMessage) ; throw new Exception("stop retry")
    }
  }

  /**
   *
   * @param cidInchikeyList
   * @return Triple DirectParent and a list of Alternative Parent
   */
  def buildCIDtypeOfChemontGraph(cidInchikeyList : Dataset[CIDAndInchiKey]): Dataset[ResultSetDirectParentAndAltPerents] = {
    implicit val cidEncoder: Encoder[ResultSetDirectParentAndAltPerents] = Encoders.product[ResultSetDirectParentAndAltPerents]
    cidInchikeyList
      .distinct()
    //  .repartition(1) /* 12 request / executions to classifyre in a same time */
      .flatMap{
        case (v) =>
        Try(retry(retryNum)(getEntityFromClassyFire(v.cid,v.inchikey))) match {
          case Success(v) => v
          case Failure(_) => None
        }
      }
    }
}
