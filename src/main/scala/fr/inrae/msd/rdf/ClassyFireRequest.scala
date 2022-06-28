package fr.inrae.msd.rdf

import org.apache.jena.graph.{NodeFactory, Triple}
import org.apache.jena.vocabulary.RDF
import org.apache.spark.rdd.RDD

import scala.language.postfixOps
import scala.util.{Failure, Success, Try}

case object ClassyFireRequest {

  val durationRetry : Int   = 10000 /* milliseconds */
  val retryNum      : Int   = 10

  def buildTriplesDirectParentAndAltParents(responseRequest : String, CID : String) : (Triple,Seq[Triple]) = {

    val data = ujson.read(responseRequest)
    val uriDirectParent : String = data("direct_parent")("chemont_id").value.toString.replace("CHEMONTID:","http://purl.obolibrary.org/obo/CHEMONTID_")
    val listUrisAltParents : Seq[String] = data("alternative_parents").arr.map( r => r("chemont_id").value.toString.replace("CHEMONTID:","http://purl.obolibrary.org/obo/CHEMONTID_") )

    val tripleDirectParent= Triple.create(
      NodeFactory.createURI(s"http://rdf.ncbi.nlm.nih.gov/pubchem/compound/$CID"),
      RDF.`type`.asNode(),
      NodeFactory.createURI(uriDirectParent)
    )

    val listTriplesAltParents =
      listUrisAltParents.map(
        uri => Triple.create(
          NodeFactory.createURI(s"http://rdf.ncbi.nlm.nih.gov/pubchem/compound/$CID"),
          RDF.`type`.asNode(),
          NodeFactory.createURI(uri)
        )
      )
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
  def getEntityFromClassyFire(CID : String , inchiKey  : String) : (Triple,Seq[Triple]) = {
    val p = requests.get(
      s"http://classyfire.wishartlab.com/entities/$inchiKey.json",
      headers=Map("Content-Type"-> "application/json")
    )
    buildTriplesDirectParentAndAltParents(p.text,CID)
  }

  // Returning a Try[T] wrapper
  // Returning T, throwing the exception on failure
  @annotation.tailrec
  final def retry[T](n: Int)(fn: => T): T = {
    Try { fn } match {
      case Success(x) => x
      case Failure(e) if n > 0 => {
        println(e.getMessage)
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
  def buildCIDtypeOfChemontGraph(cidInchikeyList : RDD[(String,String)]): RDD[(Triple, Seq[Triple])] = {

    cidInchikeyList
      .flatMap{
        case (cid,inchiKey) =>
        Try(retry(retryNum)(getEntityFromClassyFire(cid,inchiKey))) match {
          case Success(v) => Some(v)
          case Failure(_) => None
        }
      }
    }
}
