package es.upm.fi.dia.oeg.mappingpedia.test

import java.io.File

import es.upm.fi.dia.oeg.mappingpedia.controller.MappingDocumentController
import es.upm.fi.dia.oeg.mappingpedia.model.MappingDocument

object TestMappingDocumentController {
  def main(args:Array[String]) = {
    val controller = MappingDocumentController();
    this.testAddMappingDocument(controller);

    println("bye");
  }

  def testAddMappingDocument(controller: MappingDocumentController) = {
    val organizationId = "test-mobileage-upm3";
    val datasetId = "becbc9ff-e2ac-47b8-8781-260d53067119";
    val datasetPackageId = "ae2025a5-86bf-4be2-a850-00e40dd88811";
    val mdDownloadUrl = "https://raw.githubusercontent.com/oeg-upm/mappingpedia-engine/master/examples/edificio-historico.r2rml.ttl";

    val manifestFile:File = null;
    val replaceMappingBaseURI:String = null
    val generateManifestFile:Boolean = true
    val mappingDocument = new MappingDocument()
    mappingDocument.setDownloadURL(mdDownloadUrl)

    controller.addNewMappingDocument(
      organizationId
      , datasetId
      , datasetPackageId
      , manifestFile
      , replaceMappingBaseURI
      , generateManifestFile
      , mappingDocument: MappingDocument
    )
  }

}
