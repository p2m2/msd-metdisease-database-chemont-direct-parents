package fr.inrae.msd.rdf
import org.apache.spark.sql.{Dataset, Encoder, Encoders}
import org.apache.jena.graph.{Node, NodeFactory, Triple}
import org.apache.jena.riot.Lang
import org.apache.jena.vocabulary.RDF

case class WriterCidChemont(rootMsdDirectory : String,forumCategoryMsd: String,versionMsd: String) {
  def write(graphDirectParent : Dataset[(String,String)],
            graphAltParents : Dataset[(Seq[String],String)]) = {

    import net.sansa_stack.rdf.spark.io._
    implicit val stringEncoder: Encoder[String] = Encoders.STRING

    graphDirectParent.rdd.map {
      case(chemontId,cid) => Triple.create(
        NodeFactory.createURI(s"http://rdf.ncbi.nlm.nih.gov/pubchem/compound/$cid"),
        RDF.`type`.asNode(),
        NodeFactory.createURI(chemontId.replace("CHEMONTID:","http://purl.obolibrary.org/obo/CHEMONTID_")))
    } saveAsNTriplesFile(s"$rootMsdDirectory/$forumCategoryMsd/ClassyFire/$versionMsd/direct_parent.ttl",mode=SaveMode.Overwrite)

    graphAltParents.rdd.flatMap {
      case(listChemontOd,cid) => listChemontOd.map(
        uri => Triple.create(
          NodeFactory.createURI(s"http://rdf.ncbi.nlm.nih.gov/pubchem/compound/$cid"),
          RDF.`type`.asNode(),
          NodeFactory.createURI(uri.replace("CHEMONTID:","http://purl.obolibrary.org/obo/CHEMONTID_"))
        )
      )
    } saveAsNTriplesFile(s"$rootMsdDirectory/$forumCategoryMsd/ClassyFire/$versionMsd/alternative_parents.ttl",mode=SaveMode.Overwrite)
  }
}
