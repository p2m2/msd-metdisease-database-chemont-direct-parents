package fr.inrae.msd.rdf

/* ToDS */
import fr.inrae.msd.rdf.ClassyFireRequest.ResultSetDirectParentAndAltPerents
import fr.inrae.semantic_web.ProvenanceBuilder
import net.sansa_stack.rdf.spark.io._
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}

import java.util.Date
/**
 * https://services.pfem.clermont.inrae.fr/gitlab/forum/metdiseasedatabase/-/blob/develop/app/build/import_PMID_CID.py
 * build/import_PMID_CID.py
 *
 * example using corese rdf4j : https://notes.inria.fr/s/OB038LBLV
 */
/*
To avoid => Exception in thread "main" java.lang.NoSuchMethodError: scala.runtime.Statics.releaseFence()V
can not extends App
 */
object DPChemontBuilder extends App {

  import scopt.OParser

  case class Config(
                     rootMsdDirectory : String = "/rdf",
                     forumCategoryMsd : String = "forum/DiseaseChem",
                     forumDatabaseMsd : String = "ClassyFire",
                     pubchemVersionMsd: Option[String] = None,
                     referenceUriPrefix: String = "http://rdf.ncbi.nlm.nih.gov/pubchem/reference/PMID",
                     packSize : Int = 5000,
                     apiKey : Option[String] = Some("30bc501ba6ab4cba2feedffb726cbe825c0a"),
                     timeout : Int = 1200,
                     verbose: Boolean = false,
                     debug: Boolean = false)

  val builder = OParser.builder[Config]
  val parser1 = {
    import builder._
    OParser.sequence(
      programName("msd-metdisease-database-chemont-parents"),
      head("msd-metdisease-database-chemont-parents", "1.0"),
      opt[String]('d', "rootMsdDirectory")
        .optional()
        .valueName("<rootMsdDirectory>")
        .action((x, c) => c.copy(rootMsdDirectory = x))
        .text("versionMsd : release of reference/pubchem database"),
      opt[String]('r', "versionMsd")
        .optional()
        .valueName("<versionMsd>")
        .action((x, c) => c.copy(pubchemVersionMsd = Some(x)))
        .text("versionMsd : release of pubchem database"),
      opt[Int]('p',"packSize")
        .optional()
        .action({ case (r, c) => c.copy(packSize = r) })
        .validate(x =>
          if (x > 0) success
          else failure("Value <packSize> must be >0"))
        .valueName("<packSize>")
        .text("packSize to request pmid/cid eutils/elink API."),
      opt[String]("apiKey")
        .optional()
        .action({ case (r, c) => c.copy(apiKey = Some(r)) })
        .valueName("<apiKey>")
        .text("apiKey to request pmid/cid eutils/elink API."),
      opt[Int]("timeout")
        .optional()
        .action({ case (r, c) => c.copy(timeout = r) })
        .validate(x =>
          if (x > 0) success
          else failure("Value <timeout> must be >0"))
        .valueName("<timeout>")
        .text("timeout to manage error request pmid/cid eutils/elink API."),
      opt[Unit]("verbose")
        .optional()
        .action((_, c) => c.copy(verbose = true))
        .text("verbose is a flag"),
      opt[Unit]("debug")
        .hidden()
        .action((_, c) => c.copy(debug = true))
        .text("this option is hidden in the usage text"),

      help("help").text("prints this usage text"),
      note("some notes." + sys.props("line.separator")),
      checkConfig(_ => success)
    )
  }
  println("********************************JAVA****************************************")
  println(Runtime.version)
  println("************************************************************************\n\n\n\n")

  val spark: SparkSession = SparkSession
    .builder()
    .appName("msd-metdisease-database-chemont-parents-builder")
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.crossJoin.enabled", "true")
    .config("spark.kryo.registrator","net.sansa_stack.rdf.spark.io.JenaKryoRegistrator")
    /*
    .config("spark.kryo.registrator", String.join(
      ", ",
      "net.sansa_stack.rdf.spark.io.JenaKryoRegistrator",
      "net.sansa_stack.query.spark.ontop.OntopKryoRegistrator",
      "net.sansa_stack.query.spark.sparqlify.KryoRegistratorSparqlify")) */
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        // do something
        println(config)
        build(
          config.rootMsdDirectory,
          config.forumCategoryMsd,
          config.forumDatabaseMsd,
          config.pubchemVersionMsd match {
            case Some(version) => version
            case None => MsdUtils(
              rootDir=config.rootMsdDirectory,
              category=config.forumCategoryMsd,
              database="PMID_CID",spark=spark).getLastVersion
          },
          config.referenceUriPrefix,
          config.packSize,
          config.apiKey match {
            case Some(apiK) => apiK
            case None => ""
          },
          config.timeout,
          config.verbose,
          config.debug)
      case _ =>
        // arguments are bad, error message will have been displayed
        System.err.println("exit with error.")
    }

  /**
   * First execution of the work.
   * Build asso PMID <-> CID and a list f PMID error
   * @param rootMsdDirectory
   * @param forumCategoryMsd
   * @param forumDatabaseMsd
   * @param categoryMsd
   * @param databaseMsd
   * @param versionMsd
   * @param referenceUriPrefix
   * @param packSize
   * @param apiKey
   * @param timeout
   * @param verbose
   * @param debug
   */
  def build(
             rootMsdDirectory : String,
             forumCategoryMsd : String,
             forumDatabaseMsd : String,
             versionMsd: String,
             referenceUriPrefix: String,
             packSize : Int,
             apiKey : String,
             timeout : Int,
             verbose: Boolean,
             debug: Boolean) : Unit = {

    val startBuild = new Date()
    println("============== Main Build ====================")

    val CID_Inchs : Dataset[CIDAndInchiKey] = extract_CID_InchiKey(rootMsdDirectory,s"$rootMsdDirectory/$forumCategoryMsd/PMID_CID/$versionMsd/pmid_cid.ttl")
    val graphs : Dataset[ResultSetDirectParentAndAltPerents]  = ClassyFireRequest.buildCIDtypeOfChemontGraph(CID_Inchs)


    implicit val stringStringEncoder: Encoder[(String,String)] = Encoders.product[(String,String)]
    implicit val seqStringStringEncoder: Encoder[(Seq[String],String)] = Encoders.product[(Seq[String],String)]

    val graphDirectParent : Dataset[(String,String)] = graphs.flatMap( _._1 )
    val graphAltParents : Dataset[(Seq[String],String)] = graphs.flatMap( _._2 )

    WriterCidChemont(rootMsdDirectory,forumCategoryMsd,versionMsd).write(graphDirectParent,graphAltParents)

    val contentProvenanceRDF : String =
      ProvenanceBuilder.provSparkSubmit(
        projectUrl ="https://github.com/p2m2/msd-metdisease-database-chemont-parents-builder",
        category = forumCategoryMsd,
        database = forumDatabaseMsd,
        release=versionMsd,
        startDate = startBuild,
        spark
      )

    MsdUtils(
      rootDir=rootMsdDirectory,
      spark=spark,
      category="prov",
      database="ClassyFire",
      version=versionMsd).writeFile(spark,contentProvenanceRDF,"msd-metdisease-database-chemont-parents-builder-"+versionMsd+".ttl")

    spark.close()
  }

  /**
   * https://github.com/eMetaboHUB/Forum-DiseasesChem/blob/master/app/build/classyfire_functions.py#L120
   * @param rootMsdDirectory
   * @param input
   */
  def extract_CID_InchiKey(rootMsdDirectory : String,input : String) : Dataset[CIDAndInchiKey] = {
    import net.sansa_stack.rdf.spark.model.TripleOperations

    val triples_asso_pmid_cid : RDD[Triple] = spark.rdf(Lang.TURTLE)(input)

    val triplesDataset : Dataset[Triple] = triples_asso_pmid_cid.toDS()

    implicit val cidEncoder: Encoder[CID] = Encoders.product[CID]

    //implicit val nodeEncoder = Encoders.kryo(classOf[Node])
    //implicit val nodeTupleEncoder = Encoders.kryo(classOf[(Node, Node, Node)])
    /**
     * 1) CID from PMID_CID
     */
    val CIDFromPmids : Dataset[CID] = triplesDataset.map(
      (triple  : Triple ) => {
        CID(triple.getObject.toString)
      }
    ).distinct()

    /**
     *
     * This function is used to retrieve all ChemOnt classes associated to each molecules in df. As these processes are run in parralel, size of each created graph need to be exported in this function.
     * This function return a table of 4 values: nb. triples in direct_parent graph file, nb. subjects in direct_parent graph file, nb. triples in Alternative_parent graph file, nb. subjects in Alternative_parent graph file
     *
     * Inchikey linked with CID
     */

    val listRdfInchikeyFiles = MsdPubChem(spark,rootDir=rootMsdDirectory).getPathInchiKey2compoundFiles()

    println("=====================================  CID/INCHI OF INTEREST ========================================= ")
    implicit val cidInchiEncoder: Encoder[CIDAndInchiKey] = Encoders.product[CIDAndInchiKey]

    spark.emptyDataset[CIDAndInchiKey].union(
      listRdfInchikeyFiles.map(pathFile => {
        val dataset: Dataset[Triple] = spark.rdf(Lang.TURTLE)(pathFile).toDS()

        val isAttributeOf : Node = NodeFactory.createURI("http://semanticscience.org/resource/is-attribute-of")

        val pubchemList  = dataset
          .filter(_.getPredicate.matches(isAttributeOf))
          .map(
            (triple  : Triple ) => {
              CIDAndInchiKey(triple.getObject.toString,triple.getSubject.toString)
            })

        //pubchemList.show(truncate=false)
        val joined = pubchemList.join(CIDFromPmids,pubchemList("cid")===CIDFromPmids("cid")) /* Get the intersection with CID linked to a PMID here !!! */

        joined.map {
          case row =>
            val cid : String  = row.getString(0)
            val inchi : String = row.getString(1)
            CIDAndInchiKey(cid.replace("http://rdf.ncbi.nlm.nih.gov/pubchem/compound/", ""),
              inchi.replace("http://rdf.ncbi.nlm.nih.gov/pubchem/inchikey/", ""))
        }
    }).reduce((x, y) => x.union(y))
    )
  }

}
