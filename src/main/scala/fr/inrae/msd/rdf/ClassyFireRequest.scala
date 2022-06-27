package fr.inrae.msd.rdf

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}
import org.apache.spark.rdd.RDD
class ClassyFireRequest {

  val durationRetry : Int   = 10000 /* milliseconds */
  val retryNum      : Int   = 10

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
  def getEntityFromClassyFire(CID : String , inchiKey  : String) : Int = {
    val p = requests.get(
      s"http://classyfire.wishartlab.com/entities/$inchiKey.json",
      headers=Map("Content-Type"-> "application/json")
    )
    println(p.text)
    0
  }

  // Returning a Try[T] wrapper
  // Returning T, throwing the exception on failure
  @annotation.tailrec
  def retry[T](n: Int)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case Failure(e) if n > 0 => {
        println(e.getMessage())
        println(s"***RETRY $n")
        Thread.sleep(durationRetry)
        retry(n - 1)(fn)
      }
      case Failure(e) => println(e.getMessage()) ; throw new Exception("stop retry")
    }
  }

  /**
   * Finding Related Data Through Entrez Links
   *
   * get PMID -> Some(List(CID)) or None
   */

  def classyFire(cidInchikeyList : RDD[(String,String)]) : Unit = {

    println("*********************************elink**************************")
    cidInchikeyList
      .map{
        case (cid,inchiKey) => {
        println("*********************************REQUEST**************************")
        Try(retry(retryNum)(getEntityFromClassyFire(cid,inchiKey))) match {
          case Success(v) => 0
          case Failure(_) => 0
        }
      } }
    }
}
