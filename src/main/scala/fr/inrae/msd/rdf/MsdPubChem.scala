package fr.inrae.msd.rdf

import net.sansa_stack.ml.spark.featureExtraction.SparqlFrame
import net.sansa_stack.query.spark.SPARQLEngine
import net.sansa_stack.query.spark.sparqlify.QueryEngineFactorySparqlify
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.Triple
import org.apache.jena.riot.Lang
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}


case class MsdPubChem(
                       spark : SparkSession,
                       rootDir:String = "/rdf",
                       versionOpt : Option[String] = None
                     ) {

  val prefixes : Map[String,String] = Map(
    ":" -> "http://rdf.ncbi.nlm.nih.gov/pubchem/void.ttl#",
    "void:" -> "http://rdfs.org/ns/void#"
  )

  val category = "pubchem"
  val database = "void"
  val version = versionOpt match {
    case Some(v) => v
    case None => MsdUtils(
      rootDir=rootDir,
      spark=spark,
      category=category,
      database=database).getLastVersion
  }

  implicit val enc: Encoder[String] = Encoders.STRING

  /* query engine */
  val queryEngineFactory = new QueryEngineFactorySparqlify(spark)

  /* void pubchem as a Dataset */
  import net.sansa_stack.rdf.spark.io._
  import net.sansa_stack.rdf.spark.model.TripleOperations

  val voidRdd : Dataset [Triple] = spark.rdf(Lang.TURTLE)(s"$rootDir/$category/$database/$version/void.ttl").toDS()
  voidRdd.map( _.toString).show(truncate=false)
  def uri(prefix: String, name : String) : String = "<"+prefixes.getOrElse(prefix,"")+name+">"

  def getPathReferenceTypeFiles() : Seq[String] = {
    val queryString : String =
      "select ?path { "+
        s"${uri(":","reference")} ${uri("void:","dataDump")} ?path . "+
        "}"

    val sparqlFrame =
      new SparqlFrame()
        .setSparqlQuery(queryString)
        .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)

    sparqlFrame.transform(voidRdd).map(
      row  => row.get(0).toString
    ).rdd
      .filter(_.contains("_type"))
      .map(_.split("/").last.replace(".gz",""))
      .collect()
      .map( s"$rootDir/$category/reference/$version/"+_)
  }

  def getPathInchiKey2compoundFiles() : Seq[String] = {
    val queryString : String =
      "select ?path { "+
        s"${uri(":","inchikey")} ${uri("void:","dataDump")} ?path . "+
        "}"

    val sparqlFrame =
      new SparqlFrame()
        .setSparqlQuery(queryString)
        .setQueryExcecutionEngine(SPARQLEngine.Sparqlify)

    sparqlFrame.transform(voidRdd).map(
      row  => row.get(0).toString
    ).rdd
      .filter(_.contains("pc_inchikey2compound"))
      .map(_.split("/").last.replace(".gz",""))
      .collect()
      .map( s"$rootDir/$category/inchikey/$version/"+_)
  }

}
