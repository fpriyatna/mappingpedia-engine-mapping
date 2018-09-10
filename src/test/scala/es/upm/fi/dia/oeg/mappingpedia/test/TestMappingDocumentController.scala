package es.upm.fi.dia.oeg.mappingpedia.test

import java.io.File

import es.upm.fi.dia.oeg.mappingpedia.controller.MappingDocumentController
import es.upm.fi.dia.oeg.mappingpedia.model.MappingDocument

object TestMappingDocumentController {
  val controller = MappingDocumentController();

  def main(args:Array[String]) = {
    //this.testAddMappingDocument();
    //this.controller.findAll(true);
    this.controller.findByDatasetId("4f9085c2-8f87-4a05-a375-4e3fc1bd0b0a", true);
    println("bye");
  }

  def testAddMappingDocument() = {
    val organizationId = "test-mobileage-upm3";
    val datasetId = "becbc9ff-e2ac-47b8-8781-260d53067119";
    val datasetPackageId = "ae2025a5-86bf-4be2-a850-00e40dd88811";
    val mdDownloadUrl = "https://raw.githubusercontent.com/oeg-upm/mappingpedia-engine/master/examples/edificio-historico.r2rml.ttl";

    val manifestFile:File = null;
    val replaceMappingBaseURI:String = null
    val generateManifestFile:Boolean = true
    val mappingDocument = new MappingDocument()
    mappingDocument.setDownloadURL(mdDownloadUrl)

    this.controller.addNewMappingDocument(
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
