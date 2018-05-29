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
    val organizationId = "test-mobileage-upm";
    val datasetId = "86c720d0-74b7-4273-ac2e-accf33a1b7db";
    val datasetPackageId = "3c6490fd-4a3d-4932-9335-7ff2d9ae2796";
    val manifestFile:File = null;
    val replaceMappingBaseURI:String = null
    val generateManifestFile:Boolean = true
    val mappingDocument = new MappingDocument()
    mappingDocument.setDownloadURL("https://raw.githubusercontent.com/oeg-upm/mappingpedia-contents/master/zaragoza_spain/zaragoza-cementerios/zaragoza-cementerios.rml.ttl")

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
