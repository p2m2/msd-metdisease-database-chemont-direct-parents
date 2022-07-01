package fr.inrae.msd.rdf
import org.apache.spark.sql.{Dataset, Encoder, Encoders}

case class WriterCidChemont(rootMsdDirectory : String,forumCategoryMsd: String,versionMsd: String) {
  def write(graphDirectParent : Dataset[(String,String)],
            graphAltParents : Dataset[(Seq[String],String)]) = {

    import net.sansa_stack.rdf.spark.io._
    implicit val stringEncoder: Encoder[String] = Encoders.STRING

    graphDirectParent.map{
      case(chemontId,cid) =>
        val subject = s"<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/$cid>"
        val predicate="<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
        val objec = "<"+chemontId.replace("CHEMONTID:","http://purl.obolibrary.org/obo/CHEMONTID_")+">"

        s"$subject $predicate $objec ."
    }(stringEncoder).write
      .format("text")
      .mode("overwrite")
      .save(s"$rootMsdDirectory/$forumCategoryMsd/ClassyFire/$versionMsd/direct_parent.ttl")

    graphAltParents.flatMap {
      case(listChemontOd,cid) => listChemontOd.map(
        uri => {
          val subject = s"<http://rdf.ncbi.nlm.nih.gov/pubchem/compound/$cid>"
          val predicate="<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
          val objec = "<"+uri.replace("CHEMONTID:","http://purl.obolibrary.org/obo/CHEMONTID_")+">"
          s"$subject $predicate $objec ."
        }
      )
    }.write
      .format("text")
      .mode("overwrite")
      .save(s"$rootMsdDirectory/$forumCategoryMsd/ClassyFire/$versionMsd/alternative_parents.ttl")

  }
}
